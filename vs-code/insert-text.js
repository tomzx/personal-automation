module.exports = (text) => {
	const vscode = require('vscode');

	const activeTextEditor = vscode.window.activeTextEditor;
	const position =  activeTextEditor.selection.active;
	activeTextEditor.edit((editBuilder) => {
		editBuilder.insert(position, text);
	});
};
