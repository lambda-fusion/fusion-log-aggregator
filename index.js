require('dotenv').config()
const AWS = require('aws-sdk')
const fetch = require('node-fetch')
const cwl = new AWS.CloudWatchLogs({
  region: 'eu-central-1'
})

const dbUser = process.env.DB_USER
const dbPassword = process.env.DB_PW


const gatherData = async () => {
  const response = await fetch('https://fusion-config.s3.eu-central-1.amazonaws.com/fusionConfiguration.json')
  const fusionConfig = await response.json()
  let result = {}
  await Promise.all(fusionConfig.map(async (fusionGroup) => {
    const logGroupName =  `/aws/lambda/${fusionGroup.entry}`
    const logStreams = await cwl.describeLogStreams({logGroupName, limit: 5, orderBy: 'LastEventTime', descending: true}).promise()
    //console.log(logStreams)

    for(const logStream of logStreams.logStreams){
      let latestLogStream = {}
      let lastToken;
      do {
        lastToken = latestLogStream.nextForwardToken
        const params2 = {
          logGroupName, 
          logStreamName: logStream.logStreamName,
          nextToken: latestLogStream.nextForwardToken
        }
        latestLogStream = await cwl.getLogEvents(params2).promise()
        //console.log(fusionGroup.entry, latestLogStream)
        if(latestLogStream.events.length === 0){
          continue
        }

        for(const logEvent of latestLogStream.events) {
          if(logEvent.message.startsWith('REPORT')){
            const parts = logEvent.message.split('\t', 5)
            const requestId = /REPORT RequestId: (.*)/.exec(parts[0])[1]
            const duration = parseFloatWith(/Duration: (.*) ms/i, parts[1])
            const billedDuration = parseFloatWith(/Billed Duration: (.*) ms/i, parts[2])
            const memorySize = parseFloatWith(/Memory Size: (.*) MB/i, parts[3])
            const memoryUsed = parseFloatWith(/Max Memory Used: (.*) MB/i, parts[4])
            let coldStart
            if(parts[5]) {
              coldStart = parseFloatWith(/Init Duration: (.*) ms/i, parts[5])
            }
            Object.keys(result).map((key) => {
              if(Object.keys(result[key].invocationInformation).includes(requestId)){
                result[key].invocationInformation[requestId] = Object.assign(result[key].invocationInformation[requestId] || {},{
                  duration,
                  billedDuration,
                  memorySize,
                  memoryUsed,
                  coldStart,
                  lambdaName: fusionGroup.entry 
                })
              }
            })
            continue
          }
          try {
            const msg = logEvent.message.split('INFO').slice(-1)[0]
            const parsed = JSON.parse(msg)
            const requestId = logEvent.message.split('INFO')[0].split('\t')[1]
            const starttime = parsed.starttime
            const endtime = parsed.endtime
            const traceId = parsed.traceId
            if(result[traceId]) {
              const obj = starttime ? {starttime} : {endtime}
              result[traceId].invocationInformation[requestId] = Object.assign(result[traceId].invocationInformation[requestId] || {}, obj)
            } else {
              result[traceId] = {traceId, starttime: '', endtime: '', invocationInformation: { [requestId]: { starttime, endtime }}}
            }
          } catch (err) {
          }
        }
      } while(latestLogStream.nextForwardToken !== lastToken)
    }
  }))

  Object.entries(result).map(([traceId, value]) => {
    if(Object.values(value.invocationInformation).length !== fusionConfig.length) {
      delete result[traceId]
      return
    }
    // find biggest endtime
    Object.values(value.invocationInformation).reduce((prev, curr) => {
      if(!curr.endtime){
        return prev
      }
      if(curr.endtime && (curr.endtime > prev)){
        value.endtime = curr.endtime
        return curr.endtime
      }
      return prev
    }, 0)
    // find smallest starttime
    Object.values(value.invocationInformation).reduce((prev, curr) => {
      if(!curr.starttime){
        return prev
      }
      if(curr.starttime < prev){
        value.starttime = curr.starttime
        return curr.starttime
      }
      return prev
    }, Number.MAX_SAFE_INTEGER)
    const invocationInformationArr = Object.values(value.invocationInformation)
    value.runtime = value.endtime - value.starttime
    value.totalMemoryUsed = calcSum(invocationInformationArr, 'memoryUsed')
    value.totalMemoryAllocated = calcSum(invocationInformationArr, 'memorySize')
    value.totalBilledDuration = calcSum(invocationInformationArr, 'billedDuration')
    value.totalDuration = calcSum(invocationInformationArr, 'duration')
  })
  //console.dir(result, { depth: null })

  console.log('Collected all data')
  
  return result
}

const writeToDb = async (result) => {
    const MongoClient = require('mongodb').MongoClient;
    const uri = `mongodb+srv://${dbUser}:${dbPassword}@fusion-db-ul7hq.mongodb.net/test?retryWrites=true&w=majority`
    const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true })
    await client.connect()
    const collection = client.db('fusion').collection('results')
    await collection.createIndex({ traceId: 1 }, { unique: true })
    try {
      // perform actions on the collection object
      await collection.insertMany(Object.values(result), { ordered: false })
    } catch(err){
      return err.writeErrors
    } finally {
      client.close()
    }
}

const parseFloatWith = (regex, input) => {
  const res = regex.exec(input)
  return parseFloat(res[1])
}

const calcSum = (arr, key) => {
  return arr.reduce((prev, curr) => {
    return prev + curr[key]
  }, 0)
}


module.exports.handler = async () => {
  const result = await gatherData()
  const errors = await writeToDb(result) || []
  console.log(`Done. Inserted ${Object.values(result).length - errors.length} new entries to DB`)
  return `Done. Inserted ${Object.values(result).length - errors.length} new entries to DB`
}


