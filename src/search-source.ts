import {
	DataClient,
	Logger,
	SavedAlbumArtist,
	SavedArtistTrack,
	SavedAttribute,
	SearchEntityQuery,
	SearchFilter,
	SearchQuery,
	SearchSort,
	SearchSource,
	SearchSourceApiContext,
	SearchSourceCapabilities,
	SearchSourceResults,
	SortMethod,
	StringSearchFilter,
} from "@pipe-bomb/plugin-sdk";
import MiniSearch from "minisearch";

const BATCH_SIZE = 100;
const REINDEX_INTERVAL_MS = 15 * 60 * 1000;

interface ArtistDoc {
	id: string;
	name: string;
}

interface AlbumDoc {
	id: string;
	title: string;
	artist: string;
}

interface TrackDoc {
	id: string;
	title: string;
	artist: string;
}

interface ArtistData {
	name: string;
	dateAdded: Date;
}

interface AlbumData {
	title: string;
	artist: string;
	dateAdded: Date;
}

interface TrackData {
	title: string;
	artist: string;
	dateAdded: Date;
}

function getStringAttr(
	attributes: SavedAttribute[] | null,
	key: string,
): string {
	if (!attributes) {
		return "";
	}
	const attr = attributes.find((a) => a.key === key && a.type === "string");
	if (!attr) {
		return "";
	}
	const values = attr.values as string[];
	return values[0] ?? "";
}

function joinArtistNames(
	artists: (SavedAlbumArtist | SavedArtistTrack)[],
): string {
	return [...artists]
		.sort((a, b) => a.ordinal - b.ordinal)
		.map((a) => getStringAttr(a.artist?.attributes ?? null, "name"))
		.filter(Boolean)
		.join(", ");
}

function makeMiniSearch<T extends ArtistDoc | AlbumDoc | TrackDoc>(
	fields: (keyof T & string)[],
): MiniSearch<T> {
	return new MiniSearch<T>({
		fields: fields as string[],
		storeFields: [],
		idField: "id",
		searchOptions: {
			fuzzy: 0.2,
			prefix: true,
		},
	});
}

export class PipeBombSearchSource implements SearchSource {
	readonly id = "search";

	private dataClient: DataClient;
	private logger: Logger;

	private artistIndex = makeMiniSearch<ArtistDoc>(["name"]);
	private albumIndex = makeMiniSearch<AlbumDoc>(["title", "artist"]);
	private trackIndex = makeMiniSearch<TrackDoc>(["title", "artist"]);

	private artistData = new Map<string, ArtistData>();
	private albumData = new Map<string, AlbumData>();
	private trackData = new Map<string, TrackData>();

	private artistsByName: string[] = [];
	private artistsByDate: string[] = [];
	private albumsByTitle: string[] = [];
	private albumsByArtist: string[] = [];
	private albumsByDate: string[] = [];
	private tracksByTitle: string[] = [];
	private tracksByArtist: string[] = [];
	private tracksByDate: string[] = [];

	private indexing = false;

	constructor(dataClient: DataClient, logger: Logger) {
		this.dataClient = dataClient;
		this.logger = logger;
	}

	enable(_context: SearchSourceApiContext): void {
		setInterval(() => {
			this.rebuildIndex().catch((err) => {
				this.logger.error("Periodic re-index failed:", err);
			});
		}, REINDEX_INTERVAL_MS);
	}

	getName(): string {
		return "Built-in Search";
	}

	getCapabilities(entities: {
		tracks?: boolean;
		albums?: boolean;
		artists?: boolean;
	}): SearchSourceCapabilities {
		const onlyArtists =
			entities.artists && !entities.tracks && !entities.albums;

		const sortMethods: SortMethod[] = [
			{ key: "title", ascending: true, descending: true },
			{ key: "date-added", ascending: true, descending: true },
		];
		if (!onlyArtists) {
			sortMethods.push({ key: "artist", ascending: true, descending: true });
		}

		return {
			sortMethods,
			filterableAttributes: [
				{
					entityType: "track",
					attributeKey: "title",
					attributeType: "string",
					label: "Title",
					supportsFuzzy: true,
				},
				{
					entityType: "track",
					attributeKey: "artist",
					attributeType: "string",
					label: "Artist",
					supportsFuzzy: true,
				},
				{
					entityType: "album",
					attributeKey: "title",
					attributeType: "string",
					label: "Title",
					supportsFuzzy: true,
				},
				{
					entityType: "album",
					attributeKey: "artist",
					attributeType: "string",
					label: "Artist",
					supportsFuzzy: true,
				},
				{
					entityType: "artist",
					attributeKey: "name",
					attributeType: "string",
					label: "Name",
					supportsFuzzy: true,
				},
			],
		};
	}

	async search(query: SearchQuery): Promise<SearchSourceResults> {
		const results: SearchSourceResults = {};

		if (query.entities.artists) {
			const r = this.resolveEntity(
				"artist",
				query.query,
				query.sort,
				query.entities.artists,
				query.filters,
			);
			results.artists = r.uuids;
			results.artistTotal = r.total;
		}

		if (query.entities.albums) {
			const r = this.resolveEntity(
				"album",
				query.query,
				query.sort,
				query.entities.albums,
				query.filters,
			);
			results.albums = r.uuids;
			results.albumTotal = r.total;
		}

		if (query.entities.tracks) {
			const r = this.resolveEntity(
				"track",
				query.query,
				query.sort,
				query.entities.tracks,
				query.filters,
			);
			results.tracks = r.uuids;
			results.trackTotal = r.total;
		}

		return results;
	}

	async buildIndex(onProgress?: (percent: number) => void): Promise<void> {
		if (this.indexing) {
			return;
		}
		await this.rebuildIndex(onProgress);
	}

	private async rebuildIndex(
		onProgress?: (percent: number) => void,
	): Promise<void> {
		if (this.indexing) {
			return;
		}
		this.indexing = true;
		try {
			onProgress?.(0);
			const artistResult = await this.indexArtists();
			onProgress?.(30);
			const albumResult = await this.indexAlbums();
			onProgress?.(60);
			const trackResult = await this.indexTracks();
			onProgress?.(90);

			this.artistIndex = artistResult.index;
			this.artistData = artistResult.data;
			this.albumIndex = albumResult.index;
			this.albumData = albumResult.data;
			this.trackIndex = trackResult.index;
			this.trackData = trackResult.data;

			this.buildSortedArrays();
			onProgress?.(100);

			this.logger.log(
				`Index built: ${this.artistData.size} artists, ` +
					`${this.albumData.size} albums, ${this.trackData.size} tracks`,
			);
		} catch (err) {
			this.logger.error("Failed to build search index:", err);
		} finally {
			this.indexing = false;
		}
	}

	private async indexArtists(): Promise<{
		index: MiniSearch<ArtistDoc>;
		data: Map<string, ArtistData>;
	}> {
		const index = makeMiniSearch<ArtistDoc>(["name"]);
		const data = new Map<string, ArtistData>();
		const docs: ArtistDoc[] = [];

		const count = await this.dataClient.getArtistCount();

		for (let offset = 0; offset < count; offset += BATCH_SIZE) {
			const uuids = await this.dataClient.getArtistUuids(BATCH_SIZE, offset);
			const artists = await this.dataClient.getArtists(uuids, {
				relations: { attributes: true },
			});

			for (const artist of artists) {
				const name = getStringAttr(artist.attributes, "name");
				docs.push({ id: artist.uuid, name });
				data.set(artist.uuid, { name, dateAdded: artist.dateAdded });
			}
		}

		await index.addAllAsync(docs, { chunkSize: 100 });
		return { index, data };
	}

	private async indexAlbums(): Promise<{
		index: MiniSearch<AlbumDoc>;
		data: Map<string, AlbumData>;
	}> {
		const index = makeMiniSearch<AlbumDoc>(["title", "artist"]);
		const data = new Map<string, AlbumData>();
		const docs: AlbumDoc[] = [];

		const count = await this.dataClient.getAlbumCount();

		for (let offset = 0; offset < count; offset += BATCH_SIZE) {
			const uuids = await this.dataClient.getAlbumUuids(BATCH_SIZE, offset);
			const albums = await this.dataClient.getAlbums(uuids, {
				relations: {
					attributes: true,
					artists: { attributes: true },
				},
			});

			for (const album of albums) {
				const title = getStringAttr(album.attributes, "title");
				const artist = album.artists
					? joinArtistNames(album.artists as SavedAlbumArtist[])
					: "";
				docs.push({ id: album.uuid, title, artist });
				data.set(album.uuid, { title, artist, dateAdded: album.dateAdded });
			}
		}

		await index.addAllAsync(docs, { chunkSize: 100 });
		return { index, data };
	}

	private async indexTracks(): Promise<{
		index: MiniSearch<TrackDoc>;
		data: Map<string, TrackData>;
	}> {
		const index = makeMiniSearch<TrackDoc>(["title", "artist"]);
		const data = new Map<string, TrackData>();
		const docs: TrackDoc[] = [];

		const libraryIds = this.dataClient.getLibraryHandlerIds();

		for (const { pluginId, libraryId } of libraryIds) {
			const batch: { pluginId: string; libraryId: string; trackId: string }[] =
				[];
			const batchUuids: string[] = [];

			const processCurrentBatch = async (): Promise<void> => {
				if (batch.length === 0) {
					return;
				}
				const tracks = await this.dataClient.getTracks(batch, {
					relations: {
						attributes: true,
						artists: { attributes: true },
					},
				});

				for (const track of tracks) {
					const title = getStringAttr(track.attributes, "title") || track.title;
					const artist = track.artists
						? joinArtistNames(track.artists as SavedArtistTrack[])
						: "";
					docs.push({ id: track.uuid, title, artist });
					data.set(track.uuid, { title, artist, dateAdded: track.dateAdded });
				}

				batch.length = 0;
				batchUuids.length = 0;
			};

			await this.dataClient.forEachTrackId(
				pluginId,
				libraryId,
				async (trackId, _trackUuid) => {
					batch.push({ pluginId, libraryId, trackId });
					batchUuids.push(_trackUuid);

					if (batch.length >= BATCH_SIZE) {
						await processCurrentBatch();
					}
				},
			);

			await processCurrentBatch();
		}

		await index.addAllAsync(docs, { chunkSize: 100 });
		return { index, data };
	}

	private buildSortedArrays(): void {
		this.artistsByName = [...this.artistData.entries()]
			.sort(([, a], [, b]) => a.name.localeCompare(b.name))
			.map(([uuid]) => uuid);

		this.artistsByDate = [...this.artistData.entries()]
			.sort(([, a], [, b]) => a.dateAdded.getTime() - b.dateAdded.getTime())
			.map(([uuid]) => uuid);

		this.albumsByTitle = [...this.albumData.entries()]
			.sort(([, a], [, b]) => a.title.localeCompare(b.title))
			.map(([uuid]) => uuid);

		this.albumsByArtist = [...this.albumData.entries()]
			.sort(
				([, a], [, b]) =>
					a.artist.localeCompare(b.artist) || a.title.localeCompare(b.title),
			)
			.map(([uuid]) => uuid);

		this.albumsByDate = [...this.albumData.entries()]
			.sort(([, a], [, b]) => a.dateAdded.getTime() - b.dateAdded.getTime())
			.map(([uuid]) => uuid);

		this.tracksByTitle = [...this.trackData.entries()]
			.sort(([, a], [, b]) => a.title.localeCompare(b.title))
			.map(([uuid]) => uuid);

		this.tracksByArtist = [...this.trackData.entries()]
			.sort(
				([, a], [, b]) =>
					a.artist.localeCompare(b.artist) || a.title.localeCompare(b.title),
			)
			.map(([uuid]) => uuid);

		this.tracksByDate = [...this.trackData.entries()]
			.sort(([, a], [, b]) => a.dateAdded.getTime() - b.dateAdded.getTime())
			.map(([uuid]) => uuid);
	}

	private resolveEntity(
		type: "artist" | "album" | "track",
		queryText: string | undefined,
		sort: SearchSort | undefined,
		entityQuery: SearchEntityQuery,
		filters: SearchFilter[] | undefined,
	): { uuids: string[]; total: number } {
		const { limit, page = 1, allowedUuids } = entityQuery;
		const offset = (page - 1) * limit;

		const entityFilters = (filters ?? []).filter(
			(f): f is StringSearchFilter =>
				f.entityType === type && f.attributeType === "string",
		);

		const allowedSet = allowedUuids ? new Set(allowedUuids) : null;

		let candidates: string[];

		if (!queryText?.trim()) {
			candidates = this.getSortedArray(type, sort);
		} else {
			const index =
				type === "artist"
					? this.artistIndex
					: type === "album"
						? this.albumIndex
						: this.trackIndex;

			const msResults = index.search(queryText);
			candidates = msResults.map((r) => r.id as string);

			if (sort) {
				candidates = this.sortUuids(type, candidates, sort);
			}
		}

		if (allowedSet) {
			candidates = candidates.filter((uuid) => allowedSet.has(uuid));
		}

		if (entityFilters.length > 0) {
			candidates = this.applyStringFilters(type, candidates, entityFilters);
		}

		return {
			uuids: candidates.slice(offset, offset + limit),
			total: candidates.length,
		};
	}

	private applyStringFilters(
		type: "artist" | "album" | "track",
		uuids: string[],
		filters: StringSearchFilter[],
	): string[] {
		return uuids.filter((uuid) => {
			return filters.every((filter) => {
				const val = filter.value?.toLowerCase();
				if (!val) {
					return true;
				}

				if (type === "artist") {
					const d = this.artistData.get(uuid);
					if (!d) {
						return false;
					}
					if (
						filter.attributeKey === "title" ||
						filter.attributeKey === "name"
					) {
						return d.name.toLowerCase().includes(val);
					}
					return true;
				}

				if (type === "album") {
					const d = this.albumData.get(uuid);
					if (!d) {
						return false;
					}
					if (filter.attributeKey === "title") {
						return d.title.toLowerCase().includes(val);
					}
					if (filter.attributeKey === "artist") {
						return d.artist.toLowerCase().includes(val);
					}
					return true;
				}

				// track
				const d = this.trackData.get(uuid);
				if (!d) {
					return false;
				}
				if (filter.attributeKey === "title") {
					return d.title.toLowerCase().includes(val);
				}
				if (filter.attributeKey === "artist") {
					return d.artist.toLowerCase().includes(val);
				}
				return true;
			});
		});
	}

	private getSortedArray(
		type: "artist" | "album" | "track",
		sort: SearchSort | undefined,
	): string[] {
		const key = sort?.key;
		const dir = sort?.direction ?? "asc";

		if (type === "artist") {
			if (key === "date-added") {
				return dir === "asc"
					? this.artistsByDate
					: [...this.artistsByDate].reverse();
			}
			return dir === "asc"
				? this.artistsByName
				: [...this.artistsByName].reverse();
		}

		if (type === "album") {
			if (key === "artist") {
				return dir === "asc"
					? this.albumsByArtist
					: [...this.albumsByArtist].reverse();
			}
			if (key === "date-added") {
				return dir === "asc"
					? this.albumsByDate
					: [...this.albumsByDate].reverse();
			}
			return dir === "asc"
				? this.albumsByTitle
				: [...this.albumsByTitle].reverse();
		}

		// track
		if (key === "artist") {
			return dir === "asc"
				? this.tracksByArtist
				: [...this.tracksByArtist].reverse();
		}
		if (key === "date-added") {
			return dir === "asc"
				? this.tracksByDate
				: [...this.tracksByDate].reverse();
		}
		return dir === "asc"
			? this.tracksByTitle
			: [...this.tracksByTitle].reverse();
	}

	private sortUuids(
		type: "artist" | "album" | "track",
		uuids: string[],
		sort: SearchSort,
	): string[] {
		const { key, direction } = sort;
		const dir = direction === "asc" ? 1 : -1;

		return [...uuids].sort((a, b) => {
			if (type === "artist") {
				const aD = this.artistData.get(a);
				const bD = this.artistData.get(b);
				if (!aD || !bD) {
					return 0;
				}
				if (key === "date-added") {
					return (aD.dateAdded.getTime() - bD.dateAdded.getTime()) * dir;
				}
				return aD.name.localeCompare(bD.name) * dir;
			}

			if (type === "album") {
				const aD = this.albumData.get(a);
				const bD = this.albumData.get(b);
				if (!aD || !bD) {
					return 0;
				}
				if (key === "artist") {
					return (
						(aD.artist.localeCompare(bD.artist) ||
							aD.title.localeCompare(bD.title)) * dir
					);
				}
				if (key === "date-added") {
					return (aD.dateAdded.getTime() - bD.dateAdded.getTime()) * dir;
				}
				return aD.title.localeCompare(bD.title) * dir;
			}

			// track
			const aD = this.trackData.get(a);
			const bD = this.trackData.get(b);
			if (!aD || !bD) {
				return 0;
			}
			if (key === "artist") {
				return (
					(aD.artist.localeCompare(bD.artist) ||
						aD.title.localeCompare(bD.title)) * dir
				);
			}
			if (key === "date-added") {
				return (aD.dateAdded.getTime() - bD.dateAdded.getTime()) * dir;
			}
			return aD.title.localeCompare(bD.title) * dir;
		});
	}
}
