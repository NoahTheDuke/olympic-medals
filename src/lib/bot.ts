import { bskyAccount, bskyService } from "./config.js";
import type { AppBskyFeedPost, AtpAgentLoginOpts, AtpAgentOptions } from "@atproto/api";
import { AtpAgent, RichText } from "@atproto/api";

interface BotOptions {
	service: string | URL;
	dryRun: boolean;
}

export type TempRecord = Partial<AppBskyFeedPost.Record> &
	Pick<AppBskyFeedPost.Record, "text">;

export default class Bot {
	#agent;

	static defaultOptions: BotOptions = {
		service: bskyService,
		dryRun: false,
	} as const;

	constructor(service: AtpAgentOptions["service"]) {
		this.#agent = new AtpAgent({ service });
	}

	login(loginOpts: AtpAgentLoginOpts) {
		return this.#agent.login(loginOpts);
	}

	async post(record: TempRecord) {
		return this.#agent.post(record);
	}

	async buildRecord(post: string | TempRecord): Promise<TempRecord> {
		if (typeof post === "string") {
			const richText = new RichText({ text: post });
			await richText.detectFacets(this.#agent);
			return {
				text: richText.text,
				facets: richText.facets,
			};
		} else {
			return {
				text: post.text.trim(),
				facets: post.facets,
				embed: post.embed,
			};
		}
	}

	static getOptions(botOptions?: Partial<BotOptions>) {
		const { service, dryRun } = botOptions
			? Object.assign({}, this.defaultOptions, botOptions)
			: this.defaultOptions;
		return { service, dryRun };
	}

	static async run(
		post: undefined | string | TempRecord,
		botOptions?: Partial<BotOptions>,
	) {
		if (!post) return;
		const { service, dryRun } = Bot.getOptions(botOptions);
		const bot = new Bot(service);
		await bot.login(bskyAccount);
		const record = await bot.buildRecord(post);
		if (dryRun) {
			console.log(post);
		} else {
			await bot.post(record);
		}
		return record;
	}
}
