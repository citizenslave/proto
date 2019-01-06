// Proto Coding Challenge

var fs = require('fs');

const DATA_FILE = 'txnlog.dat';
const USER_QUERY = '2456938384156277127';

const MAGIC_STRING = 'MPS7'
const MAGIC_STRING_SIZE = MAGIC_STRING.length;
const VERSION_SIZE = 1;
const RECORD_COUNT_SIZE = 4

const MAGIC_STRING_OFFSET = 0;
const VERSION_OFFSET = MAGIC_STRING_OFFSET+MAGIC_STRING_SIZE;
const RECORD_COUNT_OFFSET = VERSION_OFFSET+VERSION_SIZE;

const HEADER_OFFSET = MAGIC_STRING_OFFSET;
const HEADER_SIZE = MAGIC_STRING_SIZE+VERSION_SIZE+RECORD_COUNT_SIZE;
const TXNS_OFFSET = HEADER_OFFSET+HEADER_SIZE;

const TXN_TYPE_DATA_SIZE = 1;
const TS_DATA_SIZE = 4;
const UID_DATA_SIZE = 8;
const AMT_DATA_SIZE = 8;

const TXN_TYPE_OFFSET = 0;
const TS_OFFSET = TXN_TYPE_OFFSET+TXN_TYPE_DATA_SIZE;
const UID_OFFSET = TS_OFFSET+TS_DATA_SIZE;
const AMT_OFFSET = UID_OFFSET+UID_DATA_SIZE;

const TXN_DATA_SIZE_AP = TXN_TYPE_DATA_SIZE+TS_DATA_SIZE+UID_DATA_SIZE;
const TXN_DATA_SIZE = TXN_DATA_SIZE_AP+AMT_DATA_SIZE;

const TXN_DEBIT = 0x00;
const TXN_CREDIT = 0x01;
const TXN_START_AUTOPAY = 0x02;
const TXN_END_AUTOPAY = 0x03;

console.log('\n\\\\\\\\\n');

//test(DATA_FILE, USER_QUERY);

/////////////////

function test(fileName, userQuery) {
	console.log('DATA FILE:\t\t'+fileName);
	console.log('USER QUERY:\t\t'+userQuery);
	console.log('MAGIC STRING:\t\t'+MAGIC_STRING);
	processTransactionFile(fileName, function(txnData, recordCount) {
		console.log('Testing Assertions...');
		console.assert(txnData[0].type === TXN_DEBIT);
		console.assert(txnData[0].timestamp === 1393108945);
		console.assert(txnData[0].userId === '4136353673894269217');
		console.assert(txnData[0].amount === 604.274335557087);
		console.log('Assertions Completed.');
		
		console.log('TOTAL RECORDS:\t\t'+recordCount);
		
		console.log('TOTAL DEBITS:\t\t$%s', totals(txnData, TXN_DEBIT).toFixed(2));
		console.log('TOTAL CREDITS:\t\t$%s', totals(txnData, TXN_CREDIT).toFixed(2));
		console.log('STARTED AUTOPAYS:\t'+countTxnType(txnData, TXN_START_AUTOPAY));
		console.log('ENDED AUTOPAYS:\t\t'+countTxnType(txnData, TXN_END_AUTOPAY));
		console.log('\nBALANCE FOR USER (%s)\n\t($%s)', USER_QUERY, getUserBalance(txnData, USER_QUERY).toFixed(2));
	});
}

function countTxnType(txnData, type) {
	var count = 0;
	for (var i in txnData) {
		if (txnData[i].type === type) {
			count++;
		}
	}
	
	return count;
}

function totals(txnData, type) {
	var balance = 0;
	for (var i in txnData) {
		if (txnData[i].type === type) {
			balance += txnData[i].amount;
		}
	}
	
	return balance;
}

function getUserBalance(txnData, userId) {
	var balance = 0.0;
	for (var i in txnData) {
		if (txnData[i].userId == userId) {
			if (txnData[i].type === TXN_DEBIT) {
				balance -= txnData[i].amount;
			}
			if (txnData[i].type === TXN_CREDIT) {
				balance += txnData[i].amount;
			}
		}
	}
	
	return balance;
}

///////////////////

function processTransactionFile(fileName, callback) {
	openTransactionFile(fileName, function(recordCount, file) {
		parseTransactions(file, recordCount, function(txnData) {
			callback(txnData, recordCount);
			fs.closeSync(file);
		});
	});
}

function openTransactionFile(fileName, callback) {
	openFile(fileName, function(file) {
		readHeader(file, function(headerData) {
			var recordCount = headerData.recordCount;
			callback(recordCount, file);
		});
	});
}

function openFile(fileName, callback) {
	fs.open(fileName, 'r', function(status, fd) {
		if (status !== null) {
			throw 'Could not open transaction data file:\n'+(status.message);
		} else {
			callback(fd);
		}
	});
}

function readHeader(file, callback) {
	fs.read(file, Buffer.alloc(HEADER_SIZE), HEADER_OFFSET, HEADER_SIZE, HEADER_OFFSET, function(error, bytesRead, buffer) {
		if (error !== null) {
			throw 'Error Reading File Header:\n'+(error.message);
		}
		if (bytesRead !== HEADER_SIZE) {
			throw 'Could Not Read Full Header';
		}
		if (buffer.toString('utf8', MAGIC_STRING_OFFSET, MAGIC_STRING_SIZE) !== MAGIC_STRING) {
			throw 'Invalid File Format (Unexpected Magic String)';
		}
		
		var headerData = {
			magicString: buffer.toString('utf8', MAGIC_STRING_OFFSET, MAGIC_STRING_SIZE),
			version: buffer.readInt8(VERSION_OFFSET),
			recordCount: buffer.readUInt32BE(RECORD_COUNT_OFFSET)
		};
		
		callback(headerData);
	});
}

function parseTransactions(file, expectedRecordCount, callback) {
	var transactions = [];
	
	readTransactions(file, 0, transactions, expectedRecordCount, callback);
}

function readTransactions(file, index, transactionsArray, count, callback) {
	count -= 1;
	fs.read(file, Buffer.alloc(TXN_DATA_SIZE), 0, TXN_DATA_SIZE, TXNS_OFFSET+index, function(error, bytesRead, buffer) {
		parseTransaction(buffer, function(txnData, txnSize) {
			transactionsArray.push(txnData);
			if (count > 0) {
				readTransactions(file, index+txnSize, transactionsArray, count, callback);
			} else {
				callback(transactionsArray);
			}
		});
	});
}

function parseTransaction(buffer, callback) {
	var txnSize = TXN_DATA_SIZE_AP;
	var transactionData = {
		type: buffer.readInt8(TXN_TYPE_OFFSET),
		timestamp: buffer.readUInt32BE(TS_OFFSET),
		userId: ((BigInt(buffer.readUInt32BE(UID_OFFSET)) << 32n) + BigInt(buffer.readUInt32BE(UID_OFFSET+4))).toString()
	};
		
	if (transactionData.type === TXN_DEBIT || transactionData.type === TXN_CREDIT) {
		txnSize = TXN_DATA_SIZE;
		transactionData.amount = buffer.readDoubleBE(AMT_OFFSET);
	}
	
	callback(transactionData, txnSize);
}

module.exports.process = processTransactionFile;
module.exports.runTests = test;