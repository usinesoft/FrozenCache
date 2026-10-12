import { ChangeDetectionStrategy, Component, computed, input, signal } from '@angular/core';
import { CollectionInfo, MinuteStatistics } from '../cache-api';
import {
  formatBytes,
  formatCompact,
  formatInteger,
  formatMicroseconds,
  formatTime,
} from '../format';
import { ChartBucket, ColumnChart } from '../column-chart/column-chart';

const MINUTE = 60_000;

/** Minutes per column, by range: about 60-100 columns whatever the range, so that each stays readable. */
const BUCKET_MINUTES: Record<number, number> = { 60: 1, 360: 5, 1440: 15 };

interface Bucket {
  start: number;
  end: number;
  calls: number;
  objects: number;
  /** Weighted by calls: the true average lookup time over the bucket. Null without lookups. */
  averageMicroseconds: number | null;
}

/** One collection: its description, and its lookup statistics over the selected time range. */
@Component({
  selector: 'fc-collection-card',
  imports: [ColumnChart],
  templateUrl: './collection-card.html',
  styleUrl: './collection-card.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class CollectionCard {
  readonly info = input.required<CollectionInfo>();
  readonly minutes = input.required<MinuteStatistics[]>();
  readonly rangeMinutes = input.required<number>();
  /** Time of the data, in ms: the range ends with the bucket containing it. */
  readonly now = input.required<number>();

  protected readonly showTable = signal(false);

  protected readonly formatInteger = formatInteger;
  protected readonly formatBytes = formatBytes;
  protected readonly formatMicroseconds = formatMicroseconds;
  protected readonly formatCallsTick = formatCompact;
  protected readonly formatCalls = (value: number) =>
    `${formatInteger(value)} lookup${value === 1 ? '' : 's'}`;
  protected readonly formatDurationTick = (value: number) =>
    value === 0 ? '0' : formatMicroseconds(value);

  protected readonly primaryKey = computed(() => this.info().keys[0] ?? '');
  protected readonly secondaryKeys = computed(() => this.info().keys.slice(1));

  protected readonly bucketMinutes = computed(() => BUCKET_MINUTES[this.rangeMinutes()] ?? 1);

  protected readonly callsTitle = computed(() =>
    this.bucketMinutes() === 1 ? 'Lookups per minute' : `Lookups per ${this.bucketMinutes()} minutes`,
  );

  /** The range as consecutive buckets aligned on the clock, oldest first; the last one contains "now". */
  private readonly buckets = computed<Bucket[]>(() => {
    const size = this.bucketMinutes() * MINUTE;
    const count = Math.round(this.rangeMinutes() / this.bucketMinutes());
    const lastStart = Math.floor(this.now() / size) * size;
    const firstStart = lastStart - (count - 1) * size;

    const buckets: Bucket[] = Array.from({ length: count }, (_, i) => ({
      start: firstStart + i * size,
      end: firstStart + (i + 1) * size,
      calls: 0,
      objects: 0,
      averageMicroseconds: null,
    }));

    const weightedMicroseconds = new Array<number>(count).fill(0);
    for (const minute of this.minutes()) {
      const index = Math.floor((Date.parse(minute.minute) - firstStart) / size);
      if (index < 0 || index >= count) continue;
      buckets[index].calls += minute.calls;
      buckets[index].objects += minute.retrievedObjects;
      weightedMicroseconds[index] += minute.averageMicroseconds * minute.calls;
    }

    buckets.forEach((bucket, i) => {
      if (bucket.calls > 0) bucket.averageMicroseconds = weightedMicroseconds[i] / bucket.calls;
    });
    return buckets;
  });

  /** A bucket without lookups has no column in either chart: 0 lookups, and no duration at all. */
  protected readonly callsSeries = computed<ChartBucket[]>(() =>
    this.buckets().map(({ start, end, calls }) => ({ start, end, value: calls })),
  );

  protected readonly durationSeries = computed<ChartBucket[]>(() =>
    this.buckets().map(({ start, end, averageMicroseconds }) => ({
      start,
      end,
      value: averageMicroseconds,
    })),
  );

  protected readonly totals = computed(() => {
    let calls = 0;
    let weightedMicroseconds = 0;
    for (const bucket of this.buckets()) {
      calls += bucket.calls;
      weightedMicroseconds += (bucket.averageMicroseconds ?? 0) * bucket.calls;
    }
    return { calls, averageMicroseconds: calls > 0 ? weightedMicroseconds / calls : null };
  });

  /** Buckets with lookups, most recent first: the same values as the charts. */
  protected readonly tableRows = computed(() =>
    this.buckets()
      .filter((b) => b.calls > 0)
      .reverse()
      .map((b) => ({
        ...b,
        label:
          this.bucketMinutes() === 1
            ? formatTime(b.start)
            : `${formatTime(b.start)}–${formatTime(b.end)}`,
      })),
  );
}
