import { bskyAccount, bskyService } from "./config.js";
import type {
	AppBskyFeedPost,
	AtpAgentLoginOpts,
	AtpAgentOptions,
	Facet,
} from "@atproto/api";
import { AtpAgent, RichText } from "@atproto/api";

interface BotOptions {
	service: string | URL;
	dryRun: boolean;
}

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

	async post(record: { text: string; facets: Facet[] | undefined }) {
		return this.#agent.post(record);
	}

	async buildRecord(
		post: string | { text: string; facets: Facet[] },
	): Promise<{ text: string; facets: Facet[] | undefined }> {
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
			};
		}
	}

	static async run(
		post: undefined | string | { text: string; facets: Facet[] },
		botOptions?: Partial<BotOptions>,
	) {
		if (!post) return;
		const { service, dryRun } = botOptions
			? Object.assign({}, this.defaultOptions, botOptions)
			: this.defaultOptions;
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
