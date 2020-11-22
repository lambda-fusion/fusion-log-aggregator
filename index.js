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

const gatherData = async (isS3Event) => {
  const configUrl = isS3Event
    ? `${fusionConfigURL.split("/").slice(0, -1).join("/")}/config_old.json`
    : fusionConfigURL;
  const response = await fetch(configUrl);
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
                if (currentTraceId === "6a204932-71eb-48e1-8010-2c13f675bb75") {
                  console.log(result[currentTraceId]);
                }
              });
              continue;
            }

            if (
              logEvent.message.includes("ERROR") ||
              logEvent.message.includes("Task timed out after")
            ) {
              console.log(
                "Application has thrown error or timed out",
                logEvent.message,
                currentTraceId
              );
              if (currentTraceId) {
                Object.assign(result[currentTraceId] || {}, {
                  error: {
                    message: logEvent.message,
                  },
                });
              } else {
                console.log("no trace id belonging to current error", logEvent);
              }
              continue;
            }

            const msg = logEvent.message.split("INFO").slice(-1)[0];
            // try to parse as json
            let parsed;
            try {
              parsed = JSON.parse(msg);
            } catch (err) {
              continue;
            }

            const requestId = logEvent.message.split("INFO")[0].split("\t")[1];
            const starttime = parsed.starttime;
            const endtime = parsed.endtime;
            currentTraceId = parsed.traceId;

            // console.log(
            //   result[currentTraceId].invocationInformation[requestId]
            //     .starttime
            // );
            // add current entry to existing result object
            if (result[currentTraceId]) {
              const currentInvocationInformation =
                result[currentTraceId].invocationInformation[requestId] || {};
              const currentStarttime = currentInvocationInformation.starttime;
              const currentEndtime = currentInvocationInformation.endtime;

              const invocationObject = starttime
                ? {
                    starttime: Math.min(
                      starttime,
                      currentStarttime || Number.MAX_VALUE
                    ),
                  }
                : {
                    endtime: Math.max(
                      endtime,
                      currentEndtime || Number.MIN_VALUE
                    ),
                  };
              result[currentTraceId].invocationInformation[
                requestId
              ] = Object.assign(
                currentInvocationInformation || {},
                invocationObject
              );
            } else {
              result[currentTraceId] = {
                traceId: currentTraceId,
                starttime: "",
                endtime: "",
                invocationInformation: {
                  [requestId]: {
                    starttime,
                    endtime,
                  },
                },
              };
            }
          }
        } while (latestLogStream.nextForwardToken !== lastToken);
      }
    })
  );

  Object.entries(result).map(([traceId, entry]) => {
    // // ensure all functions finished,
    // if (
    //   Object.values(entry.invocationInformation).length < fusionConfig.length
    // ) {
    //   console.log(
    //     "invocation information smaller than fusion config",
    //     entry.invocationInformation,
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
    if (entry.endtime && entry.starttime) {
      entry.runtime = entry.endtime - entry.starttime;
    }
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
    await collection.updateMany(Object.values(result), {
      upsert: true,
    });
  } catch (err) {
    return err.writeErrors;
  } finally {
    client.close();
  }
};

module.exports.handler = async (event) => {
  const isS3Event = !!event.Records;
  console.log("is s3 event?", isS3Event);
  const result = await gatherData(isS3Event);
  const errors = (await writeToDb(result)) || [];
  console.log(
    `Done. Updated ${Object.values(result).length - errors.length} entries`
  );
  return `Done. Updated ${
    Object.values(result).length - errors.length
  } new entries`;
};
