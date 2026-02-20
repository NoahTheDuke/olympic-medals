import Bot from "./lib/bot.js";
import { getPostText, testAll } from "./lib/getPostText.js";

if (!!process.env.RUNTEST) {
	await testAll();
} else {
	const opts = { dryRun: !!process.env.DRYRUN };
	const post = await getPostText();
	const text = await Bot.run(post, opts);

	if (!opts.dryRun) {
		if (text) {
			console.log(`[${new Date().toISOString()}] Posted: "${text}"`);
		} else {
			console.log("Couldn't find a suitable result");
		}
	}
}
