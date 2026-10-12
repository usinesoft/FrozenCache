import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  ElementRef,
  afterNextRender,
  computed,
  inject,
  input,
  signal,
} from '@angular/core';
import { formatTime } from '../format';

/** One time bucket of a series. A null value means no data for the bucket: no column is drawn. */
export interface ChartBucket {
  /** Start of the bucket, ms. */
  start: number;
  /** End of the bucket (exclusive), ms. */
  end: number;
  value: number | null;
}

const HEIGHT = 150;
const MARGIN = { top: 10, right: 12, bottom: 22, left: 48 };

const MAX_COLUMN_WIDTH = 24;
const COLUMN_GAP = 2;
const CORNER_RADIUS = 4;

/** Candidate spacings of the time labels, in minutes; the smallest one giving at most 5 labels is used. */
const TIME_TICK_STEPS = [5, 10, 15, 30, 60, 120, 180, 240, 360];

/**
 * Single-series column chart over time buckets, in plain SVG: columns at most 24px wide with a 4px rounded top and a
 * square base, 2px apart, on a hairline grid. Each bucket's whole slot is its hover target (pointer or keyboard:
 * arrows, Home, End); the hovered column lightens and a tooltip gives its value and time span.
 */
@Component({
  selector: 'fc-column-chart',
  templateUrl: './column-chart.html',
  styleUrl: './column-chart.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: { '[class]': '"series-" + series()' },
})
export class ColumnChart {
  readonly title = input.required<string>();
  readonly buckets = input.required<ChartBucket[]>();
  /** Selects the series color token. */
  readonly series = input.required<'calls' | 'duration'>();
  readonly formatValue = input.required<(value: number) => string>();
  readonly formatTick = input.required<(value: number) => string>();
  /** Tooltip text for a bucket without data. */
  readonly gapLabel = input('No data');
  readonly emptyMessage = input('No data in this period');
  /** The last bucket contains the current minute, still accumulating. */
  readonly lastIsPartial = input(false);

  protected readonly height = HEIGHT;
  protected readonly left = MARGIN.left;
  protected readonly top = MARGIN.top;
  protected readonly bottom = HEIGHT - MARGIN.bottom;

  protected readonly width = signal(400);
  protected readonly right = computed(() => this.width() - MARGIN.right);

  protected readonly hoverIndex = signal<number | null>(null);

  constructor() {
    // measured rather than scaled through a viewBox, so that text keeps its size and stays crisp
    const host = inject<ElementRef<HTMLElement>>(ElementRef).nativeElement;
    const observer = new ResizeObserver((entries) =>
      this.width.set(Math.max(240, Math.round(entries[0].contentRect.width))),
    );
    afterNextRender(() => observer.observe(host));
    inject(DestroyRef).onDestroy(() => observer.disconnect());
  }

  protected readonly peak = computed(() =>
    Math.max(0, ...this.buckets().map((b) => b.value ?? 0)),
  );

  protected readonly hasData = computed(() => this.peak() > 0);

  private readonly maxValue = computed(() => niceCeiling(this.peak()));

  /** Horizontal space per bucket. */
  private readonly slot = computed(() => {
    const count = this.buckets().length;
    return count > 0 ? (this.right() - this.left) / count : 0;
  });

  private readonly columnWidth = computed(() =>
    Math.max(1, Math.min(MAX_COLUMN_WIDTH, this.slot() - COLUMN_GAP)),
  );

  private slotLeft(index: number): number {
    return this.left + index * this.slot();
  }

  private center(index: number): number {
    return this.slotLeft(index) + this.slot() / 2;
  }

  private y(value: number): number {
    return this.top + (this.bottom - this.top) * (1 - value / this.maxValue());
  }

  protected readonly yTicks = computed(() => {
    const max = this.maxValue();
    return [0, max / 2, max].map((value) => ({
      value,
      y: this.y(value),
      label: this.formatTick()(value),
    }));
  });

  protected readonly xTicks = computed(() => {
    const buckets = this.buckets();
    if (buckets.length < 2) return [];

    const rangeMinutes = (buckets[buckets.length - 1].end - buckets[0].start) / 60_000;
    const every = TIME_TICK_STEPS.find((s) => rangeMinutes / s <= 5) ?? 360;

    return buckets.flatMap((bucket, index) => {
      const date = new Date(bucket.start);
      if ((date.getHours() * 60 + date.getMinutes()) % every !== 0) return [];

      const x = this.center(index);
      // labels near the edges are anchored inwards instead of overflowing
      const anchor = x - this.left < 20 ? 'start' : this.right() - x < 20 ? 'end' : 'middle';
      return [{ time: bucket.start, x, anchor, label: formatTime(bucket.start) }];
    });
  });

  /** One path per non-empty bucket: rounded at the data end, square on the baseline. */
  protected readonly columns = computed(() => {
    const width = this.columnWidth();
    return this.buckets().flatMap((bucket, index) => {
      if (bucket.value === null || bucket.value <= 0) return [];

      const x = this.center(index) - width / 2;
      const top = this.y(bucket.value);
      const radius = Math.min(CORNER_RADIUS, width / 2, this.bottom - top);
      const d =
        `M${x},${this.bottom}V${top + radius}` +
        `Q${x},${top} ${x + radius},${top}H${x + width - radius}` +
        `Q${x + width},${top} ${x + width},${top + radius}V${this.bottom}Z`;
      return [{ index, d }];
    });
  });

  protected readonly hover = computed(() => {
    const index = this.hoverIndex();
    const buckets = this.buckets();
    if (index === null || index >= buckets.length) return null;

    const bucket = buckets[index];
    const x = this.center(index);
    const partial = this.lastIsPartial() && index === buckets.length - 1;
    const singleMinute = bucket.end - bucket.start <= 60_000;

    return {
      index,
      x,
      bandX: this.slotLeft(index),
      bandWidth: this.slot(),
      valueLabel: bucket.value === null ? this.gapLabel() : this.formatValue()(bucket.value),
      timeLabel:
        (singleMinute
          ? formatTime(bucket.start)
          : `${formatTime(bucket.start)}–${formatTime(bucket.end)}`) +
        (partial ? ' · in progress' : ''),
      // keep the tooltip inside the chart
      align: x > this.width() * 0.65 ? 'end' : x < this.width() * 0.35 ? 'start' : 'middle',
    };
  });

  protected readonly ariaLabel = computed(() =>
    this.hasData()
      ? `${this.title()}, peak ${this.formatValue()(this.peak())}. Use the arrow keys to read each column.`
      : `${this.title()}: ${this.emptyMessage()}`,
  );

  protected onPointerMove(event: PointerEvent): void {
    const svg = event.currentTarget as SVGSVGElement;
    const x = event.clientX - svg.getBoundingClientRect().left;
    this.hoverIndex.set(this.indexAt(x));
  }

  protected onKeyDown(event: KeyboardEvent): void {
    const last = this.buckets().length - 1;
    const current = this.hoverIndex() ?? last;
    const next =
      event.key === 'ArrowLeft' ? current - 1
      : event.key === 'ArrowRight' ? current + 1
      : event.key === 'Home' ? 0
      : event.key === 'End' ? last
      : event.key === 'Escape' ? null
      : undefined;

    if (next === undefined) return;
    event.preventDefault();
    this.hoverIndex.set(next === null ? null : Math.min(last, Math.max(0, next)));
  }

  protected onFocus(): void {
    if (this.hoverIndex() === null) this.hoverIndex.set(this.buckets().length - 1);
  }

  /** The bucket whose slot contains x: the whole slot is the hit target, not just the painted column. */
  private indexAt(x: number): number | null {
    const count = this.buckets().length;
    const slot = this.slot();
    if (count === 0 || slot <= 0) return null;
    return Math.min(count - 1, Math.max(0, Math.floor((x - this.left) / slot)));
  }
}

/** Smallest "round" number (1, 2, 2.5, 5 × 10^n) >= value, so that axis ticks are clean. */
function niceCeiling(value: number): number {
  if (value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const fraction = value / magnitude;
  const nice = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 2.5 ? 2.5 : fraction <= 5 ? 5 : 10;
  return nice * magnitude;
}
