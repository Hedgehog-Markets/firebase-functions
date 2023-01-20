// The MIT License (MIT)
//
// Copyright (c) 2022 Firebase
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

import * as express from "express";
import { DecodedIdToken } from "firebase-admin/auth";

import * as logger from "../../logger";
import * as https from "./https";
import { Expression } from "../../params";
import { ResetValue } from "../options";

/** How a task should be retried in the event of a non-2xx return. */
export interface RetryConfig {
  /**
   * Maximum number of times a request should be attempted.
   * If left unspecified, will default to 3.
   */
  maxAttempts?: number | Expression<number> | ResetValue;

  /**
   * Maximum amount of time for retrying failed task.
   * If left unspecified will retry indefinitely.
   */
  maxRetrySeconds?: number | Expression<number> | ResetValue;

  /**
   * The maximum amount of time to wait between attempts.
   * If left unspecified will default to 1hr.
   */
  maxBackoffSeconds?: number | Expression<number> | ResetValue;

  /**
   * The maximum number of times to double the backoff between
   * retries. If left unspecified will default to 16.
   */
  maxDoublings?: number | Expression<number> | ResetValue;

  /**
   * The minimum time to wait between attempts. If left unspecified
   * will default to 100ms.
   */
  minBackoffSeconds?: number | Expression<number> | ResetValue;
}

/** How congestion control should be applied to the function. */
export interface RateLimits {
  /**
   * The maximum number of requests that can be outstanding at a time.
   * If left unspecified, will default to 1000.
   */
  maxConcurrentDispatches?: number | Expression<number> | ResetValue;

  /**
   * The maximum number of requests that can be invoked per second.
   * If left unspecified, will default to 500.
   */
  maxDispatchesPerSecond?: number | Expression<number> | ResetValue;
}

/** Metadata about the authorization used to invoke a function. */
export interface AuthData {
  uid: string;
  token: DecodedIdToken;
}

/** Metadata about the task invocation. */
export interface TaskData {
  /**
   * The name of the queue.
   */
  queue: string;

  /**
   * The "short" name of the task, or, if no name was specified at creation,
   * a unique system-generated id.
   *
   * This is the `{task}` value in the complete task name, ie.
   * `projects/{project}/locations/{location}/queues/{queue}/tasks/{task}`.
   */
  name: string;

  /**
   * The number of times this task has been retried. For the first attempt,
   * this value is `0`. This number includes attempts where the task failed
   * due to `5XX` error codes and never reached the execution phase.
   */
  retryCount: number;

  /**
   * The total number of times that the task has received a response from the
   * handler. Since Cloud Tasks deletes the task once a successful response
   * has been received, all previous handler responses were failures.
   * This number does not include failures due to 5XX error codes.
   */
  executionCount: number;

  /**
   * The schedule time of the task, specified in seconds since January 1st 1970.
   */
  eta: number;
}

/** Metadata about a call to a Task Queue function. */
export interface TaskContext {
  /**
   * The result of decoding and verifying an ODIC token.
   */
  auth?: AuthData;

  /**
   * Task-specific information.
   */
  task?: TaskData;
}

/**
 * The request used to call a Task Queue function.
 */
export interface Request<T = any> {
  /**
   * The parameters used by a client when calling this function.
   */
  data: T;

  /**
   * The result of decoding and verifying an ODIC token.
   */
  auth?: AuthData;

  /**
   * Task-specific information.
   */
  task?: TaskData;
}

type v1TaskHandler = (data: any, context: TaskContext) => void | Promise<void>;
type v2TaskHandler<Req> = (request: Request<Req>) => void | Promise<void>;

/** @internal */
export function onDispatchHandler<Req = any>(
  handler: v1TaskHandler | v2TaskHandler<Req>
): (req: https.Request, res: express.Response) => Promise<void> {
  return async (req: https.Request, res: express.Response): Promise<void> => {
    try {
      if (!https.isValidRequest(req)) {
        logger.error("Invalid request, unable to process.");
        throw new https.HttpsError("invalid-argument", "Bad Request");
      }

      const context: TaskContext = {};
      if (!process.env.FUNCTIONS_EMULATOR) {
        const authHeader = req.header("Authorization") || "";
        const token = authHeader.match(/^Bearer (.*)$/)?.[1];
        // Note: this should never happen since task queue functions are guarded by IAM.
        if (!token) {
          throw new https.HttpsError("unauthenticated", "Unauthenticated");
        }
        // We skip authenticating the token since tq functions are guarded by IAM.
        const authToken = https.unsafeDecodeIdToken(token);
        context.auth = {
          uid: authToken.uid,
          token: authToken,
        };

        const invalidHeaders: [string, string | null][] = [];
        const queueName = req.header("X-CloudTasks-QueueName");
        if (queueName == null) {
          invalidHeaders.push(["X-CloudTasks-QueueName", null]);
        }
        const taskName = req.header("X-CloudTasks-TaskName");
        if (taskName == null) {
          invalidHeaders.push(["X-CloudTasks-TaskName", null]);
        }
        const retryCount = req.header("X-CloudTasks-TaskRetryCount");
        if (retryCount == null || !/^\d+$/.test(retryCount)) {
          invalidHeaders.push(["X-CloudTasks-TaskRetryCount", null]);
        }
        const executionCount = req.header("X-CloudTasks-TaskExecutionCount");
        if (executionCount == null || !/^\d+$/.test(executionCount)) {
          invalidHeaders.push(["X-CloudTasks-TaskExecutionCount", executionCount ?? null]);
        }
        const taskEta = req.header("X-CloudTasks-TaskETA");
        if (taskEta == null || !/^\d+$/.test(taskEta)) {
          invalidHeaders.push(["X-CloudTasks-TaskETA", taskEta ?? null]);
        }
        if (invalidHeaders.length > 0) {
          logger.error("Invalid cloud task headers", { headers: invalidHeaders });
          throw new https.HttpsError("internal", "Invalid cloud task headers", invalidHeaders);
        }
        context.task = {
          queue: queueName,
          name: taskName,
          retryCount: Number(retryCount),
          executionCount: Number(executionCount),
          eta: Number(taskEta),
        };
      }

      const data: Req = https.decode(req.body.data);
      if (handler.length === 2) {
        await handler(data, context);
      } else {
        const arg: Request<Req> = {
          ...context,
          data,
        };
        // For some reason the type system isn't picking up that the handler
        // is a one argument function.
        await (handler as v2TaskHandler<Req>)(arg);
      }

      res.status(204).end();
    } catch (err) {
      let httpErr: https.HttpsError = err;
      if (!(err instanceof https.HttpsError)) {
        // This doesn't count as an 'explicit' error.
        logger.error("Unhandled error", err);
        httpErr = new https.HttpsError("internal", "INTERNAL");
      }

      const { status } = httpErr.httpErrorCode;
      const body = { error: httpErr.toJSON() };

      res.status(status).send(body);
    }
  };
}
