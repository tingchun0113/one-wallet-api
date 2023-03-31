import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({});
const dynamo = DynamoDBDocumentClient.from(client);
const [balancesTable, transactionsTable] = ['balances', 'transactions'];
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
const handleNotExistBet = (uuid) =>
  handleResponse(200, 'BET_DOES_NOT_EXIST', uuid);
const handleExistBet = (uuid) => handleResponse(200, 'BET_ALREADY_EXIST', uuid);
const handleSettledBet = (uuid) =>
  handleResponse(200, 'BET_ALREADY_SETTLED', uuid);
const handleInsufficientFunds = (uuid) =>
  handleResponse(200, 'INSUFFICIENT_FUNDS', uuid);
const handleFinalError = (uuid) =>
  handleResponse(200, 'FINAL_ERROR_ACTION_FAILED', uuid);

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

  let [isDebit, isCredit, isCancel] = [false, false, false];
  switch (requestPath) {
    case '/debit':
      isDebit = true;
      break;
    case '/credit':
      isCredit = true;
      break;
    case '/cancel':
      isCancel = true;
      break;
    default:
      break;
  }

  const { sid, userId, currency, transaction, uuid } = requestBody;
  const { refId, amount } = transaction;

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
      if (isDebit && balance - amount < 0) {
        return handleInsufficientFunds(uuid);
      }
    } else {
      return handleInvalidUserId(uuid);
    }

    const transactionsData = await dynamo.send(
      new GetCommand({
        TableName: transactionsTable,
        Key: { refId },
      })
    );

    if (transactionsData.Item?.cancel && isDebit) {
      return handleFinalError(uuid);
    }

    if (transactionsData.Item?.debit && isDebit) {
      return handleExistBet(uuid);
    }

    if (transactionsData.Item?.cancel || transactionsData.Item?.credit) {
      return handleSettledBet(uuid);
    }

    let [storedCancel, storedCredit, storedDebit] = [false, false, false];
    if (transactionsData?.Item) {
      ({
        cancel: storedCancel,
        credit: storedCredit,
        debit: storedDebit,
      } = transactionsData.Item);
    }

    await dynamo.send(
      new PutCommand({
        TableName: transactionsTable,
        Item: {
          refId,
          cancel: isCancel ? isCancel : storedCancel,
          credit: isCredit ? isCredit : storedCredit,
          debit: isDebit ? isDebit : storedDebit,
          currency,
          userId,
        },
      })
    );

    if (!transactionsData?.Item && !isDebit) {
      return handleNotExistBet(uuid);
    }

    balance += isCredit ? amount : isDebit ? -amount : isCancel ? amount : 0;
    balance = parseFloat(balance.toFixed(6));

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
