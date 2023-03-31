import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { v4 as uuidParam } from 'uuid';

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);
const usersTable = 'balances';
const owAuthToken = 's3cr3tV4lu3';

const handleResponse = (statusCode, status, uuid = null) => ({
  statusCode,
  body: JSON.stringify({ status, uuid }),
});

const handleUnauthorized = () => handleResponse(200, 'INVALID_TOKEN_ID');
const handleInvalidRequest = () => handleResponse(200, 'INVALID_PARAMETER');
const handleTemporaryError = (uuid) =>
  handleResponse(500, 'TEMPORARY_ERROR', uuid);
const handleUnknownError = (uuid) => handleResponse(500, 'UNKNOWN_ERROR', uuid);
const handleInvalidSid = (uuid) => handleResponse(200, 'INVALID_SID', uuid);
const handleInvalidUserId = (uuid) =>
  handleResponse(200, 'INVALID_PARAMETER', uuid);

const generateNewSid = () => {
  //return 'new-sid-to-be-used-for-api-calls-qwerty';
  return uuidParam();
};

const handleItemFound = async (Item, sid, userId, uuid) => {
  const { sid: storedSid, userId: storedUserId } = Item;

  if (storedSid !== sid && storedUserId !== userId) {
    return handleInvalidSid(uuid);
  }

  const newSid = generateNewSid();

  await dynamo.send(
    new UpdateCommand({
      TableName: usersTable,
      Key: {
        userId,
      },
      UpdateExpression: 'set sid = :newSid',
      ExpressionAttributeValues: {
        ':newSid': newSid,
      },
      ReturnValues: 'ALL_NEW',
    })
  );

  return {
    statusCode: 200,
    body: JSON.stringify({
      status: 'OK',
      sid: newSid,
      uuid,
    }),
  };
};

export const handler = async (event) => {
  const authToken = event.queryStringParameters?.authToken ?? null;
  if (authToken !== owAuthToken) {
    return handleUnauthorized();
  }

  const requestPath = event.path;
  const requestBody = event.body ? JSON.parse(event.body) : null;
  if (!requestPath || !requestBody) {
    return handleInvalidRequest();
  }

  const { sid, userId, uuid } = requestBody;

  try {
    const { Item } = await dynamo.send(
      new GetCommand({
        TableName: usersTable,
        Key: {
          userId,
        },
      })
    );

    if (Item) {
      return handleItemFound(Item, sid, userId, uuid);
    } else {
      return handleInvalidUserId(uuid);
    }
  } catch (error) {
    console.log(error);
    if (error.statusCode && error.statusCode !== 200) {
      return handleTemporaryError(uuid);
    }
    try {
      JSON.parse(error.message);
      return handleTemporaryError(uuid);
    } catch (e) {
      return handleUnknownError(uuid);
    }
  }
};
