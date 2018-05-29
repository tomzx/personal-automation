exports.execute = (args) => {
	const child_process = require('child_process');

	const insertText = require('./insert-text');

	child_process.exec(args.arguments[0], (err, stdout, stderr) => {
		if (err) {
			console.error(err);
			return;
		}

		insertText(stdout);
	});
}
