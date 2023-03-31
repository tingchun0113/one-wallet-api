import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);
const [balancesTable, promosTable] = ['balances', 'promos'];
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
const handleSettledBet = (uuid) =>
  handleResponse(200, 'BET_ALREADY_SETTLED', uuid);

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

  const { sid, userId, currency, promoTransaction, uuid } = requestBody;
  const { id, amount } = promoTransaction;

  try {
    const balancesData = await dynamo.send(
      new GetCommand({
        TableName: balancesTable,
        Key: { userId },
      })
    );

    let [balance, bonus] = [0, 0];

    if (balancesData?.Item) {
      const { sid: storedSid } = balancesData.Item;

      if (storedSid !== sid) {
        return handleInvalidSid(uuid);
      }

      ({ balance, bonus } = balancesData.Item);
    } else {
      return handleInvalidUserId(uuid);
    }

    const promosData = await dynamo.send(
      new GetCommand({
        TableName: promosTable,
        Key: { id },
      })
    );

    if (promosData?.Item) {
      return handleSettledBet(uuid);
    }

    await dynamo.send(
      new PutCommand({
        TableName: promosTable,
        Item: {
          id,
          amount,
          currency,
          userId,
        },
      })
    );

    bonus += amount;
    bonus = parseFloat(bonus.toFixed(6));

    await dynamo.send(
      new PutCommand({
        TableName: balancesTable,
        Item: {
          userId,
          balance,
          bonus,
          currency,
          sid,
        },
      })
    );

    return {
      statusCode: 200,
      body: JSON.stringify({
        status: 'OK',
        balance,
        bonus,
        uuid,
      }),
    };
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
