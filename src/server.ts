import https from "node:https";
import { SNSClient, SubscribeCommand } from "@aws-sdk/client-sns";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";
import express from "express";
import { jobQueue } from "./queue";
import logger from "./utils/logger";

const {
  PORT = "3001",
  HOSTNAME = "0.0.0.0",
  SNS_TOPIC_ARN,
  NGROK_URL,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  AWS_REGION,
} = process.env;

const sns = new SNSClient({
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID || "",
    secretAccessKey: AWS_SECRET_ACCESS_KEY || "",
  },
  region: AWS_REGION,
});

async function setupSNSSubscription() {
  if (!SNS_TOPIC_ARN) {
    throw new Error("SNS_TOPIC_ARN is not defined");
  }

  const endpoint = NGROK_URL ? `${NGROK_URL}/subscribe` : `http://${HOSTNAME}:${PORT}/subscribe`;
  logger.info("Setting up SNS subscription with endpoint:", endpoint);

  try {
    const subscriptionData = await sns.send(
      new SubscribeCommand({
        Protocol: "https",
        TopicArn: SNS_TOPIC_ARN,
        Endpoint: endpoint,
        ReturnSubscriptionArn: true,
      })
    );
    logger.info("SNS Subscription request sent:", {
      SubscriptionArn: subscriptionData.SubscriptionArn,
      Endpoint: endpoint,
    });
  } catch (error) {
    logger.error("Error creating SNS subscription:", error);
    if (error instanceof Error) {
      logger.error("Error details:", error.message);
    }
  }
}

async function handleSNSMessage(message: { Message: string }) {
  const s3Event = JSON.parse(message.Message);
  const { object } = s3Event.Records[0].s3;

  await jobQueue.add("process-file", { key: object.key }, { jobId: object.key });
}

export async function createServer() {
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath("/admin/queues");

  createBullBoard({
    queues: [new BullMQAdapter(jobQueue)],
    serverAdapter,
  });

  const app = express();
  app.use("/admin/queues", serverAdapter.getRouter());
  app.use(express.json());
  app.use(express.text());

  app.get("/health", (req, res) => {
    res.status(200).send("OK");
  });

  app.post("/subscribe", async (req, res) => {
    try {
      let message: {
        Type: string;
        SubscribeURL?: string;
        Message?: string;
      };

      if (typeof req.body === "string") {
        message = JSON.parse(req.body);
      } else {
        message = req.body;
      }

      if (message.Type === "SubscriptionConfirmation") {
        if (!message.SubscribeURL) {
          logger.error("Missing SubscribeURL in confirmation message");
          return res.status(400).send("Missing SubscribeURL");
        }

        try {
          await new Promise<void>((resolve, reject) => {
            if (!message.SubscribeURL) {
              logger.error("Missing SubscribeURL in confirmation message");
              return res.status(400).send("Missing SubscribeURL");
            }

            https
              .get(message.SubscribeURL, (res) => {
                let data = "";
                res.on("data", (chunk) => {
                  data = data + chunk;
                });
                res.on("end", () => resolve());
              })
              .on("error", reject);
          });
          logger.info("SNS Subscription confirmed successfully");
          return res.status(200).send("SNS Subscription confirmed");
        } catch (error) {
          logger.error("Failed to confirm subscription:", error);
          return res.status(500).send("Failed to confirm subscription");
        }
      } else if (message.Type === "Notification") {
        if (!message.Message) {
          logger.error("Missing Message in notification");
          return res.status(400).send("Missing Message in notification");
        }
        await handleSNSMessage({ Message: message.Message });
        return res.status(200).send("SNS Notification processed");
      } else {
        logger.error("Invalid message type:", message.Type);
        return res.status(400).send("Invalid SNS message type");
      }
    } catch (error) {
      logger.error("Error processing SNS message:", error);
      return res.status(500).send("Internal server error processing SNS message");
    }
  });

  await setupSNSSubscription();

  return new Promise<void>((resolve) => {
    app.listen(Number(PORT), HOSTNAME, () => {
      logger.info(`Server is listening on port ${PORT}`);
      resolve();
    });
  });
}
