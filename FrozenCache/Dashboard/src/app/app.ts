import { ChangeDetectionStrategy, Component, DestroyRef, inject, signal } from '@angular/core';
import { CacheApi, CollectionInfo, MinuteStatistics } from './cache-api';
import { CollectionCard } from './collection-card/collection-card';

interface CollectionData {
  info: CollectionInfo;
  minutes: MinuteStatistics[];
}

const REFRESH_INTERVAL_MS = 15_000;
const RANGE_STORAGE_KEY = 'frozencache.dashboard.range';

@Component({
  selector: 'app-root',
  imports: [CollectionCard],
  templateUrl: './app.html',
  styleUrl: './app.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class App {
  private readonly api = inject(CacheApi);

  protected readonly ranges = [
    { minutes: 60, label: '1 hour' },
    { minutes: 360, label: '6 hours' },
    { minutes: 1440, label: '24 hours' },
  ];

  protected readonly rangeMinutes = signal(loadRange());
  protected readonly collections = signal<CollectionData[] | null>(null);
  protected readonly updatedAt = signal<number | null>(null);
  protected readonly refreshing = signal(false);
  protected readonly error = signal<string | null>(null);

  constructor() {
    void this.refresh();
    const timer = setInterval(() => void this.refresh(), REFRESH_INTERVAL_MS);
    inject(DestroyRef).onDestroy(() => clearInterval(timer));
  }

  protected selectRange(minutes: number): void {
    this.rangeMinutes.set(minutes);
    try {
      localStorage.setItem(RANGE_STORAGE_KEY, String(minutes));
    } catch {
      // storage unavailable (private mode, blocked): the choice just isn't remembered
    }
  }

  protected formatUpdatedAt(time: number): string {
    return new Date(time).toLocaleTimeString();
  }

  private async refresh(): Promise<void> {
    if (this.refreshing()) return;
    this.refreshing.set(true);

    try {
      const infos = await this.api.collections();
      const collections = await Promise.all(
        infos.map(async (info) => ({
          info,
          // a collection dropped between the two requests just shows no statistics
          minutes: await this.api.monitoring(info.name).then(
            (m) => m.minutes,
            () => [],
          ),
        })),
      );

      collections.sort((a, b) => a.info.name.localeCompare(b.info.name));
      this.collections.set(collections);
      this.updatedAt.set(Date.now());
      this.error.set(null);
    } catch {
      // the last data stays on screen
      this.error.set('Cannot reach the FrozenCache server. Retrying…');
    } finally {
      this.refreshing.set(false);
    }
  }
}

function loadRange(): number {
  try {
    const stored = Number(localStorage.getItem(RANGE_STORAGE_KEY));
    return [60, 360, 1440].includes(stored) ? stored : 60;
  } catch {
    return 60;
  }
}
