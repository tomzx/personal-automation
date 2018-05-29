exports.execute = (args) => {
	const fs = require('fs');
	const moment = args.require('moment');
	const insertText = require('../vs-code/insert-text');

	const path = __dirname + '/id-date-time.json';

	let id = 1;
	if (fs.existsSync(path)) {
		id = fs.readFileSync(path);
	}

	const text = moment().format('YYYY-MM-DD HH:mm:ss') + ' [nid://' + id + ']' + '\n';
	insertText(text);

	++id;

	fs.writeFileSync(path, id);
};
