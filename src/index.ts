import Bot from "./lib/bot.js";
import getPostText from "./lib/getPostText.js";

const opts = { dryRun: !!process.env.DRYRUN };
const text = await Bot.run(getPostText, opts);

if (!opts.dryRun) {
	if (text) {
		console.log(`[${new Date().toISOString()}] Posted: "${text}"`);
	} else {
		console.log("Couldn't find a suitable result");
	}
}
