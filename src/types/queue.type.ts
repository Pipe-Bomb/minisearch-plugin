import { SavedAlbum, SavedArtist, SavedTrack } from "@pipe-bomb/plugin-sdk";

export type QueueItemMap = {
	track: SavedTrack;
	artist: SavedArtist;
	album: SavedAlbum;
};

export type Action = "update" | "delete";

export interface QueueItem<
	T extends keyof QueueItemMap,
	V extends QueueItemMap[T] = QueueItemMap[T],
> {
	type: T;
	item: V;
	action: Action;
}

export type AnyQueueItem = {
	[T in keyof QueueItemMap]: QueueItem<T>;
}[keyof QueueItemMap];
