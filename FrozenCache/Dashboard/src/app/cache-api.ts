import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { firstValueFrom } from 'rxjs';

/** One element of <c>GET /collections</c> (server: Messages.CollectionInformation). */
export interface CollectionInfo {
  name: string;
  count: number;
  sizeInBytes: number;
  avgObjectSize: number;
  lastVersion: string;
  /** Index names; the first one is the primary key. */
  keys: string[];
  segmentFileSize: number;
  maxObjectsPerSegment: number;
}

/** One minute of <c>GET /collections/{name}/monitoring</c>; only minutes with lookups are present. */
export interface MinuteStatistics {
  /** Start of the minute, ISO 8601 UTC. */
  minute: string;
  calls: number;
  retrievedObjects: number;
  averageMicroseconds: number;
}

export interface CollectionMonitoring {
  collection: string;
  minutes: MinuteStatistics[];
}

/** HTTP API of the FrozenCache server. Absolute paths: the dashboard is served by the same server, at /dashboard/. */
@Injectable({ providedIn: 'root' })
export class CacheApi {
  private readonly http = inject(HttpClient);

  collections(): Promise<CollectionInfo[]> {
    return firstValueFrom(this.http.get<CollectionInfo[]>('/collections'));
  }

  monitoring(collection: string): Promise<CollectionMonitoring> {
    return firstValueFrom(
      this.http.get<CollectionMonitoring>(`/collections/${encodeURIComponent(collection)}/monitoring`),
    );
  }
}
