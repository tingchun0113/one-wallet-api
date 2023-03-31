import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);
const balancesTable = 'balances';
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

  const { userId, uuid, sid } = requestBody;

  try {
    const { Item } = await dynamo.send(
      new GetCommand({
        TableName: balancesTable,
        Key: { userId },
      })
    );

    if (Item) {
      const { balance, bonus, sid: storedSid } = Item;

      if (storedSid !== sid) {
        return handleInvalidSid(uuid);
      }

      return {
        statusCode: 200,
        body: JSON.stringify({
          status: 'OK',
          balance,
          bonus,
          uuid,
        }),
      };
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
