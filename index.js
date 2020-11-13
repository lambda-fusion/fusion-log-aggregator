require("dotenv").config();
const AWS = require("aws-sdk");
const fetch = require("node-fetch");
const MongoClient = require("mongodb").MongoClient;
const { calcSum, parseFloatWith } = require("./utils");

const cwl = new AWS.CloudWatchLogs({
  region: "eu-central-1",
});

const dbUser = process.env.DB_USER;
const dbPassword = process.env.DB_PW;
const dbUrl = process.env.DB_URL;
const fusionConfigURL = process.env.DEPLOYMENT_CONFIG_URL;

const gatherData = async () => {
  const response = await fetch(fusionConfigURL);
  const fusionConfig = await response.json();
  let result = {};
  await Promise.all(
    fusionConfig.map(async (fusionGroup) => {
      const logGroupName = `/aws/lambda/${fusionGroup.entry}-stg`;
      const logStreams = await cwl
        .describeLogStreams({
          logGroupName,
          limit: 5,
          orderBy: "LastEventTime",
          descending: true,
        })
        .promise();

      for (const logStream of logStreams.logStreams) {
        let latestLogStream = {};
        let lastToken;
        let currentTraceId = "";
        do {
          lastToken = latestLogStream.nextForwardToken;
          const params2 = {
            logGroupName,
            logStreamName: logStream.logStreamName,
            nextToken: latestLogStream.nextForwardToken,
          };
          latestLogStream = await cwl.getLogEvents(params2).promise();
          // console.log(latestLogStream);
          //console.log(fusionGroup.entry, latestLogStream)
          if (latestLogStream.events.length === 0) {
            continue;
          }

          for (const logEvent of latestLogStream.events) {
            // parse AWS provided values
            if (logEvent.message.startsWith("REPORT")) {
              const parts = logEvent.message.replace("\n", "").split("\t");
              const requestId = /REPORT RequestId: (.*)/.exec(parts[0])[1];
              const duration = parseFloatWith(/Duration: (.*) ms/i, parts[1]);
              const billedDuration = parseFloatWith(
                /Billed Duration: (.*) ms/i,
                parts[2]
              );
              const memorySize = parseFloatWith(
                /Memory Size: (.*) MB/i,
                parts[3]
              );
              const memoryUsed = parseFloatWith(
                /Max Memory Used: (.*) MB/i,
                parts[4]
              );
              let coldStart;
              if (parts[5]) {
                coldStart =
                  parseFloatWith(/Init Duration: (.*) ms/i, parts[5]) || 0;
              }
              Object.keys(result).map((key) => {
                if (
                  Object.keys(result[key].invocationInformation).includes(
                    requestId
                  )
                ) {
                  result[key].invocationInformation[requestId] = Object.assign(
                    result[key].invocationInformation[requestId] || {},
                    {
                      duration,
                      billedDuration,
                      memorySize,
                      memoryUsed,
                      coldStart,
                      lambdaName: fusionGroup.entry,
                    }
                  );
                }
              });
              continue;
            }

            if (logEvent.message.includes("ERROR")) {
              console.log("Application has thrown error", logEvent.message);
              if (currentTraceId) {
                Object.assign(result[currentTraceId] || {}, {
                  error: {
                    message: logEvent.message,
                  },
                });
              } else {
                console.log("no trace id beloning to current error", logEvent);
              }
              continue;
            }

            try {
              const msg = logEvent.message.split("INFO").slice(-1)[0];
              const parsed = JSON.parse(msg);
              const requestId = logEvent.message
                .split("INFO")[0]
                .split("\t")[1];
              const starttime = parsed.starttime;
              const endtime = parsed.endtime;
              currentTraceId = parsed.traceId;

              // add current entry to existing result object
              if (result[currentTraceId]) {
                const obj = starttime ? { starttime } : { endtime };
                result[currentTraceId].invocationInformation[
                  requestId
                ] = Object.assign(
                  result[currentTraceId].invocationInformation[requestId] || {},
                  obj
                );
              } else {
                result[currentTraceId] = {
                  traceId: currentTraceId,
                  starttime: "",
                  endtime: "",
                  invocationInformation: {
                    [requestId]: { starttime, endtime },
                  },
                };
              }
            } catch (err) {}
          }
        } while (latestLogStream.nextForwardToken !== lastToken);
      }
    })
  );

  Object.entries(result).map(([traceId, entry]) => {
    // // ensure all functions finished,
    // if (
    //   Object.values(value.invocationInformation).length < fusionConfig.length
    // ) {
    //   console.log(
    //     "invocation information smaller than fusion config",
    //     value.invocationInformation,
    //     fusionConfig
    //   );
    //   delete result[traceId];
    //   return;
    // }

    // find biggest endtime
    Object.values(entry.invocationInformation).reduce((prev, curr) => {
      if (!curr.endtime) {
        return prev;
      }
      if (curr.endtime && curr.endtime > prev) {
        entry.endtime = curr.endtime;
        return curr.endtime;
      }
      return prev;
    }, 0);
    // find smallest starttime
    Object.values(entry.invocationInformation).reduce((prev, curr) => {
      if (!curr.starttime) {
        return prev;
      }
      if (curr.starttime < prev) {
        entry.starttime = curr.starttime;
        return curr.starttime;
      }
      return prev;
    }, Number.MAX_SAFE_INTEGER);
    const invocationInformationArr = Object.values(entry.invocationInformation);
    entry.runtime = entry.endtime - entry.starttime;
    entry.totalMemoryUsed = calcSum(invocationInformationArr, "memoryUsed");
    entry.totalMemoryAllocated = calcSum(
      invocationInformationArr,
      "memorySize"
    );
    entry.totalBilledDuration = calcSum(
      invocationInformationArr,
      "billedDuration"
    );
    entry.totalDuration = calcSum(invocationInformationArr, "duration");

    entry.starttime = new Date(entry.starttime);
    entry.endtime = new Date(entry.endtime);
    entry.lambdasCount = fusionConfig.length;
    entry.totalColdStartTime = calcSum(invocationInformationArr, "coldStart");
  });
  //console.dir(result, { depth: null })

  console.log("Collected all data");

  return result;
};

const writeToDb = async (result) => {
  const uri = `mongodb+srv://${dbUser}:${dbPassword}@${dbUrl}`;
  const client = new MongoClient(uri, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  await client.connect();
  const collection = client.db(process.env.DB_NAME).collection("results");
  await collection.createIndex({ traceId: 1 }, { unique: true });
  try {
    // perform actions on the collection object
    await collection.insertMany(Object.values(result), { ordered: false });
  } catch (err) {
    return err.writeErrors;
  } finally {
    client.close();
  }
};

module.exports.handler = async () => {
  const result = await gatherData();
  const errors = (await writeToDb(result)) || [];
  console.log(
    `Done. Inserted ${
      Object.values(result).length - errors.length
    } new entries to DB`
  );
  return `Done. Inserted ${
    Object.values(result).length - errors.length
  } new entries to DB`;
};
