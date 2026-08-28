//! Drawing, and translating terminal events into [`Key`].
//!
//! # The screen is a report, not a dashboard
//!
//! Every choice below follows from one idea: somebody is looking at this
//! because something is wrong, or because they just changed something and want
//! to know whether it worked. So the two facts that answer those questions —
//! is a generation pinned, and is the data on screen still live — are given
//! more room and more colour than their information content deserves, and
//! everything else is arranged so it can be scanned rather than read.
//!
//! # Column widths are computed, not delegated
//!
//! Ratatui will happily distribute columns for us and clip what does not fit.
//! It clips silently, though, and a hostname clipped without a mark is a
//! hostname somebody misreads. [`columns`] does the arithmetic so that
//! [`fit`] can put an ellipsis where the text was cut, and so that the
//! numeric columns keep a fixed width and stop jittering as their contents
//! change.

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, List, ListItem, Paragraph, Row, Sparkline, Table, Wrap,
};
use ratatui::Frame;

use crate::app::{App, Connection, Key, View};
use crate::contract::GenerationEntry;
use crate::model::{RouteRow, Sort};
use crate::rfc3339;

/// How wide the fixed columns are.
///
/// Fixed rather than proportional so that a rate ticking from `9.9` to `10.1`
/// does not shift every column to its right by one cell.
const TYPE_WIDTH: u16 = 8;
const EPS_WIDTH: u16 = 4;
const RPS_WIDTH: u16 = 9;
const ERR_WIDTH: u16 = 7;
const MS_WIDTH: u16 = 8;
/// One space between columns, nine columns.
const GAPS: u16 = 8;

/// The relative share of leftover width given to each flexible column, in
/// display order: host, path, backend, canary.
///
/// Not equal shares. Hosts and Kubernetes service names are the long values —
/// `default-http-backend` is twenty characters before anyone has named a
/// service after their team — while paths are usually `/` or `/api` and a
/// canary is a percentage and a name. Splitting the width evenly spends it on
/// the column that needs it least and truncates the two that need it most.
const FLEX_WEIGHTS: [u16; 4] = [5, 3, 5, 3];
/// The narrowest a flexible column may be squeezed before it is simply dropped
/// from the total; below this a column shows an ellipsis and nothing else.
const MIN_FLEX: u16 = 4;

/// The narrowest terminal in which the routes table fills its width exactly.
///
/// Below this, the floor above wins over the proportional split for the
/// narrower columns, so the widths add up to more than the terminal has and
/// ratatui clips the overflow. That is the right behaviour — four columns of
/// one character each would be worse — but it means "the widths sum to the
/// terminal width" is a promise only from here up.
///
/// The value follows from [`FLEX_WEIGHTS`] and [`MIN_FLEX`]; the test
/// `the_documented_minimum_width_is_the_real_one` keeps it honest.
pub const MIN_TABLE_WIDTH: u16 = 62;

/// The nine routes-table column widths for a given total width.
///
/// Returns them in display order: host, path, type, backend, endpoints, rps,
/// error rate, latency, canary.
pub fn columns(total: u16) -> [u16; 9] {
    let fixed = TYPE_WIDTH + EPS_WIDTH + RPS_WIDTH + ERR_WIDTH + MS_WIDTH + GAPS;
    // On a terminal too narrow to hold even the fixed columns there is nothing
    // sensible to show; give every flexible column its floor and let the table
    // clip. Saturating rather than subtracting: this runs on whatever size the
    // terminal happens to be, including one cell wide.
    let flexible = total.saturating_sub(fixed);

    let weight_total: u16 = FLEX_WEIGHTS.iter().sum();
    let mut widths = [0u16; 9];
    let mut assigned = 0u16;
    for (i, weight) in FLEX_WEIGHTS.iter().enumerate() {
        // The last flexible column takes the rounding remainder, so the columns
        // always add up to exactly the available width.
        let width = if i + 1 == FLEX_WEIGHTS.len() {
            flexible.saturating_sub(assigned)
        } else {
            flexible * weight / weight_total
        };
        let width = width.max(MIN_FLEX);
        assigned = assigned.saturating_add(width);
        // host, path, backend, canary sit at 0, 1, 3, 8.
        let slot = [0usize, 1, 3, 8][i];
        widths[slot] = width;
    }

    widths[2] = TYPE_WIDTH;
    widths[4] = EPS_WIDTH;
    widths[5] = RPS_WIDTH;
    widths[6] = ERR_WIDTH;
    widths[7] = MS_WIDTH;
    widths
}

/// Truncates to a width, marking the cut with an ellipsis.
///
/// A hostname that was shortened has to look shortened; the alternative is
/// `api.example` on screen when the route is `api.example.com.au`, which reads
/// as a different host rather than as a narrow terminal.
pub fn fit(text: &str, width: u16) -> String {
    let width = width as usize;
    if width == 0 {
        return String::new();
    }
    let length = text.chars().count();
    if length <= width {
        return text.to_string();
    }
    if width == 1 {
        return "…".to_string();
    }
    let kept: String = text.chars().take(width - 1).collect();
    format!("{kept}…")
}

/// Formats a rate with a precision that suits its magnitude.
///
/// Two decimals below ten, one below a thousand, none above, and `k` past ten
/// thousand. A column that always shows two decimals wastes three characters on
/// a five-digit rate and shows `0.00` for a route serving one request every
/// thirty seconds.
pub fn format_rate(rate: Option<f64>) -> String {
    match rate {
        None => "-".to_string(),
        Some(r) if !r.is_finite() => "-".to_string(),
        Some(r) if r >= 10_000.0 => format!("{:.1}k", r / 1000.0),
        Some(r) if r >= 1000.0 => format!("{r:.0}"),
        Some(r) if r >= 10.0 => format!("{r:.1}"),
        Some(r) => format!("{r:.2}"),
    }
}

/// Formats a percentage, or a dash where there is no evidence either way.
pub fn format_percent(value: Option<f64>) -> String {
    match value {
        None => "-".to_string(),
        Some(v) if !v.is_finite() => "-".to_string(),
        Some(v) if v >= 10.0 => format!("{v:.0}%"),
        Some(v) => format!("{v:.2}%"),
    }
}

/// Formats a latency in milliseconds.
pub fn format_ms(value: Option<f64>) -> String {
    match value {
        None => "-".to_string(),
        Some(v) if !v.is_finite() => "-".to_string(),
        Some(v) if v >= 1000.0 => format!("{:.1}s", v / 1000.0),
        Some(v) if v >= 100.0 => format!("{v:.0}"),
        Some(v) => format!("{v:.1}"),
    }
}

/// A duration, humanized the way the generation timeline humanizes an age.
fn format_duration(d: std::time::Duration) -> String {
    rfc3339::humanize_age(d.as_secs().min(i64::MAX as u64) as i64)
}

/// The colour an error rate earns.
///
/// Any 5xx at all is worth colour; a rate above one percent is worth bold. The
/// thresholds are deliberately low — this is a load balancer, and a percent of
/// a busy route is a lot of failed requests.
fn error_style(rate: Option<f64>) -> Style {
    match rate {
        Some(r) if r >= 1.0 => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        Some(r) if r > 0.0 => Style::default().fg(Color::Yellow),
        _ => Style::default(),
    }
}

/// Draws the whole frame.
pub fn draw(frame: &mut Frame, app: &mut App, now: std::time::Instant) {
    let pinned = app.snapshot.as_ref().and_then(crate::Snapshot::pinned);

    let [header_area, banner_area, main_area, status_area, keys_area] = Layout::vertical([
        Constraint::Length(5),
        // The banner takes a row only when there is something to say in it.
        Constraint::Length(u16::from(pinned.is_some())),
        Constraint::Min(3),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_header(frame, header_area, app, now);
    if let Some(generation) = pinned {
        draw_pinned_banner(frame, banner_area, generation);
    }

    match app.view {
        View::Routes => draw_routes(frame, main_area, app),
        View::Generations => draw_generations(frame, main_area, app, now),
    }

    draw_status(frame, status_area, app, now);
    draw_keys(frame, keys_area, app);
}

/// The top block: where, what generation, and the global rates.
fn draw_header(frame: &mut Frame, area: Rect, app: &App, now: std::time::Instant) {
    let stale = app.connection.is_lost();
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .title(Span::from(" ramjet-top ").bold().fg(Color::Cyan))
        .title(Span::from(format!(" {} ", app.url)).fg(Color::DarkGray));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let [text_area, spark_area] =
        Layout::horizontal([Constraint::Percentage(58), Constraint::Percentage(42)]).areas(inner);

    let snapshot = app.snapshot.as_ref();
    let serving = snapshot.map_or_else(|| "-".to_string(), |s| s.serving().to_string());
    let routes = snapshot.map_or(0, |s| s.routes.routes.len());
    let generations = snapshot.map_or(0, |s| s.generations.generations.len());

    // Dimming the whole block is how "this is not live" is said without
    // reflowing the layout or hiding the numbers somebody was reading.
    let base = if stale {
        Style::default().add_modifier(Modifier::DIM)
    } else {
        Style::default()
    };

    let global = app.global;
    let mut first = vec![
        Span::styled("gen ", base.fg(Color::DarkGray)),
        Span::styled(serving, base.fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled("  routes ", base.fg(Color::DarkGray)),
        Span::styled(routes.to_string(), base),
        Span::styled("  gens ", base.fg(Color::DarkGray)),
        Span::styled(generations.to_string(), base),
        Span::styled("  conns ", base.fg(Color::DarkGray)),
        Span::styled(
            global
                .active_connections
                .map_or_else(|| "-".to_string(), |c| c.to_string()),
            base,
        ),
    ];
    if stale {
        first.push(Span::styled(
            "  STALE",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ));
    }

    let second = vec![
        Span::styled("rps ", base.fg(Color::DarkGray)),
        Span::styled(
            format_rate(global.rps),
            base.fg(Color::Green).add_modifier(Modifier::BOLD),
        ),
        Span::styled("  5xx ", base.fg(Color::DarkGray)),
        Span::styled(
            format_percent(global.error_rate_percent),
            error_style(global.error_rate_percent).patch(base),
        ),
        Span::styled("  upstream ", base.fg(Color::DarkGray)),
        Span::styled(format_ms(global.avg_latency_ms), base),
        Span::styled("  up ", base.fg(Color::DarkGray)),
        Span::styled(format_duration(app.uptime(now)), base),
    ];

    frame.render_widget(
        Paragraph::new(vec![Line::from(first), Line::from(second)]),
        text_area,
    );

    draw_sparkline(frame, spark_area, app, base);
}

/// The request-rate history.
fn draw_sparkline(frame: &mut Frame, area: Rect, app: &App, base: Style) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let data: Vec<u64> = app.rps_history.iter().copied().collect();
    let peak = data.iter().copied().max().unwrap_or(0);

    let [label_area, spark_area] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("rps · last ", base.fg(Color::DarkGray)),
            Span::styled(data.len().to_string(), base.fg(Color::DarkGray)),
            Span::styled(" polls · peak ", base.fg(Color::DarkGray)),
            Span::styled(peak.to_string(), base.fg(Color::DarkGray)),
        ]))
        .alignment(Alignment::Right),
        label_area,
    );

    if data.is_empty() {
        frame.render_widget(
            Paragraph::new(Span::styled(
                "collecting…",
                base.fg(Color::DarkGray).add_modifier(Modifier::ITALIC),
            ))
            .alignment(Alignment::Right),
            spark_area,
        );
        return;
    }

    // Only the most recent points that fit; a sparkline scaled to more samples
    // than it has cells averages away the spike somebody is looking for.
    let visible = data.len().saturating_sub(spark_area.width as usize);
    frame.render_widget(
        Sparkline::default()
            .data(&data[visible..])
            .style(base.fg(Color::Green)),
        spark_area,
    );
}

/// The one thing that must never be missed.
fn draw_pinned_banner(frame: &mut Frame, area: Rect, generation: u64) {
    frame.render_widget(
        Paragraph::new(Line::from(format!(
            " PINNED to generation {generation} — new generations are NOT being served "
        )))
        .style(
            Style::default()
                .bg(Color::Red)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        area,
    );
}

/// The routes table.
fn draw_routes(frame: &mut Frame, area: Rect, app: &mut App) {
    let stale = app.connection.is_lost();
    let base = if stale {
        Style::default().add_modifier(Modifier::DIM)
    } else {
        Style::default()
    };

    let visible = app.visible_rows();
    let total = app.rows.len();
    let title = if app.filter.is_empty() {
        format!(" routes {total} ")
    } else {
        format!(" routes {}/{total} matching {:?} ", visible.len(), app.filter)
    };

    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .title(Span::from(title).bold())
        .title_bottom(Span::from(format!(
            " sorted by {} {} ",
            app.sort.as_str(),
            if app.descending { "desc" } else { "asc" }
        )).fg(Color::DarkGray));

    let inner = block.inner(area);
    let widths = columns(inner.width);

    let header = Row::new(vec![
        fit("HOST", widths[0]),
        fit("PATH", widths[1]),
        fit("TYPE", widths[2]),
        fit("BACKEND", widths[3]),
        fit("EPS", widths[4]),
        fit("RPS", widths[5]),
        fit("5XX", widths[6]),
        fit("ms", widths[7]),
        fit("CANARY", widths[8]),
    ])
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
    );

    let rows: Vec<Row> = visible
        .iter()
        .map(|row| route_row(row, &widths, base))
        .collect();

    let constraints: Vec<Constraint> = widths.iter().map(|w| Constraint::Length(*w)).collect();
    let table = Table::new(rows, constraints)
        .header(header)
        .block(block)
        .column_spacing(1)
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(table, area, &mut app.routes_state);
}

/// One route, as styled cells.
fn route_row<'a>(row: &RouteRow, widths: &[u16; 9], base: Style) -> Row<'a> {
    // A route with no endpoints behind it is a route that will 503, and it is
    // worth seeing before the error rate confirms it.
    let endpoint_style = if row.endpoints == 0 {
        base.fg(Color::Red).add_modifier(Modifier::BOLD)
    } else {
        base
    };

    let host = if row.is_new {
        // Newly appeared: no rate yet, and worth pointing at, because a route
        // that just appeared is usually the thing somebody just did.
        Span::styled(
            fit(&row.host, widths[0]),
            base.fg(Color::Green).add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(fit(&row.host, widths[0]), base)
    };

    let canary_style = if row.canary.is_some() {
        base.fg(Color::Magenta)
    } else {
        base.fg(Color::DarkGray)
    };

    Row::new(vec![
        Line::from(host),
        Line::from(Span::styled(fit(&row.path, widths[1]), base)),
        Line::from(Span::styled(
            fit(row.path_type.short(), widths[2]),
            base.fg(Color::DarkGray),
        )),
        Line::from(Span::styled(fit(&row.backend, widths[3]), base)),
        Line::from(Span::styled(row.endpoints.to_string(), endpoint_style))
            .alignment(Alignment::Right),
        Line::from(Span::styled(
            format_rate(row.rps),
            if row.is_new {
                base.fg(Color::Green)
            } else {
                base
            },
        ))
        .alignment(Alignment::Right),
        Line::from(Span::styled(
            format_percent(row.error_rate_percent),
            error_style(row.error_rate_percent).patch(base),
        ))
        .alignment(Alignment::Right),
        Line::from(Span::styled(format_ms(row.avg_latency_ms), base))
            .alignment(Alignment::Right),
        Line::from(Span::styled(
            fit(&row.canary_label(), widths[8]),
            canary_style,
        )),
    ])
}

/// The generation timeline, and the selected diff when it is expanded.
fn draw_generations(frame: &mut Frame, area: Rect, app: &mut App, now: std::time::Instant) {
    let (list_area, detail_area) = if app.expanded {
        let [list, detail] =
            Layout::vertical([Constraint::Percentage(45), Constraint::Percentage(55)])
                .areas(area);
        (list, Some(detail))
    } else {
        (area, None)
    };

    let base = if app.connection.is_lost() {
        Style::default().add_modifier(Modifier::DIM)
    } else {
        Style::default()
    };

    let serving = app.snapshot.as_ref().map_or(0, crate::Snapshot::serving);
    let pinned = app.snapshot.as_ref().and_then(crate::Snapshot::pinned);
    let now_unix = rfc3339::now_unix_seconds();

    let entries: Vec<&GenerationEntry> = app
        .snapshot
        .as_ref()
        .map(|s| s.generations.generations.iter().collect())
        .unwrap_or_default();

    let items: Vec<ListItem> = entries
        .iter()
        .map(|entry| {
            ListItem::new(Line::from(generation_spans(
                entry, serving, pinned, now_unix, base,
            )))
        })
        .collect();

    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .title(Span::from(format!(" generations {} ", entries.len())).bold())
        // Under `--read-only` the brake keys are refused, so they are not
        // offered. A UI that advertises a key and then declines it teaches
        // people to distrust the rest of the footer.
        .title_bottom(
            Span::from(if app.read_only {
                " Enter expands "
            } else {
                " Enter expands · p pins · u unpins "
            })
            .fg(Color::DarkGray),
        );

    if items.is_empty() {
        frame.render_widget(
            Paragraph::new(Span::styled(
                "no generations reported",
                base.fg(Color::DarkGray).add_modifier(Modifier::ITALIC),
            ))
            .block(block),
            list_area,
        );
    } else {
        frame.render_stateful_widget(
            List::new(items)
                .block(block)
                .highlight_style(Style::default().add_modifier(Modifier::REVERSED)),
            list_area,
            &mut app.generations_state,
        );
    }

    if let Some(detail_area) = detail_area {
        draw_generation_detail(frame, detail_area, app, base);
    }
    let _ = now;
}

/// One line in the timeline.
fn generation_spans<'a>(
    entry: &GenerationEntry,
    serving: u64,
    pinned: Option<u64>,
    now_unix: i64,
    base: Style,
) -> Vec<Span<'a>> {
    let marker = if pinned == Some(entry.generation) {
        Span::styled("P ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
    } else if entry.generation == serving {
        Span::styled("▶ ", base.fg(Color::Cyan).add_modifier(Modifier::BOLD))
    } else {
        Span::styled("  ", base)
    };

    let published = if entry.published {
        Span::styled("published  ", base.fg(Color::Green))
    } else {
        // An unpublished generation was compiled and not served. That is
        // either a pin in effect or a table that failed to apply, and both are
        // worth a colour that is not the same as "fine".
        Span::styled("unpublished", base.fg(Color::Yellow))
    };

    vec![
        marker,
        Span::styled(
            format!("{:>6}  ", entry.generation),
            base.add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:>7}  ", rfc3339::age_of(&entry.applied_at, now_unix)),
            base.fg(Color::DarkGray),
        ),
        published,
        Span::styled(
            format!("  {:<13}", entry.short_digest()),
            base.fg(Color::DarkGray),
        ),
        Span::styled(entry.diff.summary.clone(), base),
    ]
}

/// The expanded diff of the selected generation.
fn draw_generation_detail(frame: &mut Frame, area: Rect, app: &App, base: Style) {
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .title(Span::from(" diff ").bold())
        .title_bottom(Span::from(" Enter or Esc collapses ").fg(Color::DarkGray));

    let Some(entry) = app.selected_generation() else {
        frame.render_widget(
            Paragraph::new(Span::styled(
                "select a generation",
                base.fg(Color::DarkGray).add_modifier(Modifier::ITALIC),
            ))
            .block(block),
            area,
        );
        return;
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled("generation ", base.fg(Color::DarkGray)),
            Span::styled(
                entry.generation.to_string(),
                base.add_modifier(Modifier::BOLD),
            ),
            Span::styled("  applied ", base.fg(Color::DarkGray)),
            Span::styled(entry.applied_at.clone(), base),
            Span::styled("  digest ", base.fg(Color::DarkGray)),
            Span::styled(entry.digest.clone(), base.fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled("routes ", base.fg(Color::DarkGray)),
            Span::styled(entry.routes.to_string(), base),
            Span::styled("  hosts ", base.fg(Color::DarkGray)),
            Span::styled(entry.hosts.to_string(), base),
            Span::styled("  certs ", base.fg(Color::DarkGray)),
            Span::styled(entry.certs.to_string(), base),
        ]),
        Line::from(Span::styled(
            entry.diff.summary.clone(),
            base.add_modifier(Modifier::ITALIC),
        )),
    ];

    if entry.diff.is_empty() {
        lines.push(Line::from(Span::styled(
            "no itemised changes",
            base.fg(Color::DarkGray),
        )));
    }

    for (label, items) in entry.diff.categories() {
        if items.is_empty() {
            continue;
        }
        lines.push(Line::from(Span::styled(
            format!("{label} ({})", items.len()),
            base.fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )));
        for item in items {
            lines.push(Line::from(Span::styled(
                format!("  {item}"),
                base,
            )));
        }
    }

    frame.render_widget(
        Paragraph::new(lines).block(block).wrap(Wrap { trim: false }),
        area,
    );
}

/// The line above the keys: connection, filter, confirmations, notices.
///
/// One row, and a strict priority order, because these can all be true at once
/// and a status line that concatenates them is a status line nobody reads. A
/// confirmation outranks everything: it is the only one waiting on an answer.
fn draw_status(frame: &mut Frame, area: Rect, app: &App, now: std::time::Instant) {
    let line = if let Some(pending) = &app.pending {
        Line::from(Span::styled(
            format!(" {} ", pending.prompt()),
            Style::default()
                .bg(Color::Red)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ))
    } else if app.editing_filter {
        Line::from(vec![
            Span::styled(" filter ", Style::default().bg(Color::Cyan).fg(Color::Black)),
            Span::raw(" "),
            Span::styled(
                app.filter.clone(),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            // A visible caret; without it an empty filter box looks like a
            // frozen screen rather than a prompt.
            Span::styled("▏", Style::default().add_modifier(Modifier::SLOW_BLINK)),
            Span::styled(
                "  Enter keeps · Esc clears",
                Style::default().fg(Color::DarkGray),
            ),
        ])
    } else if let Some(notice) = app.current_notice() {
        let style = if notice.is_error {
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Green)
        };
        Line::from(Span::styled(format!(" {}", notice.text), style))
    } else {
        match &app.connection {
            Connection::Connecting => Line::from(Span::styled(
                format!(" connecting to {}…", app.url),
                Style::default().fg(Color::Yellow),
            )),
            Connection::Live => Line::from(vec![
                Span::styled(" ● ", Style::default().fg(Color::Green)),
                Span::styled(
                    format!(
                        "live · polling every {}",
                        format_duration(app.interval)
                    ),
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Connection::Lost {
                error,
                failures,
                ..
            } => {
                let stale = app
                    .stale_for(now)
                    .map(format_duration)
                    .unwrap_or_else(|| "?".to_string());
                Line::from(vec![
                    Span::styled(
                        " ● ",
                        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!("reconnecting ({failures} failed) · data is {stale} old · "),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(error.clone(), Style::default().fg(Color::DarkGray)),
                ])
            }
        }
    };

    frame.render_widget(Paragraph::new(line), area);
}

/// The keybinding footer.
fn draw_keys(frame: &mut Frame, area: Rect, app: &App) {
    let key = Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD);
    let label = Style::default().fg(Color::DarkGray);

    let mut spans = vec![
        Span::styled(" q", key),
        Span::styled(" quit  ", label),
        Span::styled("Tab", key),
        Span::styled(
            format!(" {}  ", app.view.toggled().title()),
            label,
        ),
    ];

    if app.view == View::Routes {
        for sort in [Sort::Rps, Sort::Errors, Sort::Latency, Sort::Host] {
            let selected = app.sort == sort;
            spans.push(Span::styled(
                sort.key().to_string(),
                if selected {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    key
                },
            ));
            spans.push(Span::styled(format!(" {}  ", sort.as_str()), label));
        }
        spans.push(Span::styled("/", key));
        spans.push(Span::styled(" filter  ", label));
    } else {
        spans.push(Span::styled("Enter", key));
        spans.push(Span::styled(" diff  ", label));
        if !app.read_only {
            spans.push(Span::styled("p", key));
            spans.push(Span::styled(" pin  ", label));
            spans.push(Span::styled("u", key));
            spans.push(Span::styled(" unpin  ", label));
        }
    }

    spans.push(Span::styled("g", key));
    spans.push(Span::styled(" refresh", label));

    if app.read_only {
        spans.push(Span::styled(
            "   read-only",
            Style::default().fg(Color::Yellow),
        ));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

/// Translates a terminal key event into the alphabet [`App`] is written
/// against.
///
/// Returns `None` for events this program does not bind, including key
/// *releases*: on Windows crossterm reports press and release both, and a UI
/// that does not filter them acts on every keystroke twice.
pub fn translate(event: KeyEvent) -> Option<Key> {
    if event.kind != KeyEventKind::Press {
        return None;
    }

    if event.modifiers.contains(KeyModifiers::CONTROL) {
        return match event.code {
            KeyCode::Char('c' | 'C') => Some(Key::CtrlC),
            _ => None,
        };
    }

    Some(match event.code {
        KeyCode::Char(c) => Key::Char(c),
        KeyCode::Backspace => Key::Backspace,
        KeyCode::Enter => Key::Enter,
        KeyCode::Esc => Key::Escape,
        KeyCode::Tab => Key::Tab,
        KeyCode::Up => Key::Up,
        KeyCode::Down => Key::Down,
        KeyCode::PageUp => Key::PageUp,
        KeyCode::PageDown => Key::PageDown,
        KeyCode::Home => Key::Home,
        KeyCode::End => Key::End,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use crate::client::Snapshot;
    use crate::contract::{
        Canary, GenerationDiff, GenerationsResponse, PathType, RouteEntry, RoutesResponse,
    };
    use crate::prom::MetricsSnapshot;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;
    use std::time::{Duration, Instant};

    #[test]
    fn column_widths_add_up_to_the_available_width() {
        for total in [MIN_TABLE_WIDTH, 80, 100, 120, 200, 500] {
            let widths = columns(total);
            let sum: u16 = widths.iter().sum::<u16>() + GAPS;
            assert_eq!(sum, total, "widths {widths:?} do not fill {total} columns");
        }
    }

    #[test]
    fn the_documented_minimum_width_is_the_real_one() {
        // Pins `MIN_TABLE_WIDTH` to the weights, so changing `FLEX_WEIGHTS`
        // without revisiting the constant fails here rather than silently
        // making the exact-fill promise false at some widths.
        let fills = |total: u16| {
            let widths = columns(total);
            widths.iter().sum::<u16>() + GAPS == total
        };
        assert!(fills(MIN_TABLE_WIDTH), "the minimum does not fill");
        assert!(
            !fills(MIN_TABLE_WIDTH - 1),
            "the minimum is not the smallest width that fills"
        );
        assert!((MIN_TABLE_WIDTH..300).all(fills), "a wider terminal stopped filling");
    }

    #[test]
    fn column_widths_hold_a_floor_on_a_terminal_too_narrow_to_fill() {
        // Below the minimum there is no set of widths that both fits and shows
        // anything, so the columns keep their floor and ratatui clips the
        // table. The requirement here is only that the arithmetic survives —
        // every one of these underflows a plain subtraction.
        for total in [0u16, 1, 5, 20, 39, MIN_TABLE_WIDTH - 1] {
            let widths = columns(total);
            assert!(
                widths.iter().all(|w| *w >= MIN_FLEX),
                "a column collapsed to nothing at width {total}: {widths:?}"
            );
        }
    }

    #[test]
    fn the_numeric_columns_keep_a_fixed_width_at_every_terminal_size() {
        for total in [60u16, 80, 120, 300] {
            let widths = columns(total);
            assert_eq!(widths[2], TYPE_WIDTH);
            assert_eq!(widths[4], EPS_WIDTH);
            assert_eq!(widths[5], RPS_WIDTH);
            assert_eq!(widths[6], ERR_WIDTH);
            assert_eq!(widths[7], MS_WIDTH);
        }
    }

    #[test]
    fn fitting_marks_where_it_cut() {
        assert_eq!(fit("short", 10), "short");
        assert_eq!(fit("exactly-ten", 11), "exactly-ten");
        assert_eq!(fit("api.example.com.au", 11), "api.exampl…");
        assert_eq!(fit("anything", 1), "…");
        assert_eq!(fit("anything", 0), "");
    }

    #[test]
    fn fitting_counts_characters_not_bytes() {
        // Five characters, more than five bytes. Cutting on bytes would slice
        // through a code point and panic.
        assert_eq!(fit("münch", 5), "münch");
        assert_eq!(fit("münchen", 5), "münc…");
    }

    #[test]
    fn rates_get_a_precision_that_suits_their_size() {
        assert_eq!(format_rate(None), "-");
        assert_eq!(format_rate(Some(0.0)), "0.00");
        assert_eq!(format_rate(Some(0.033)), "0.03");
        assert_eq!(format_rate(Some(9.99)), "9.99");
        assert_eq!(format_rate(Some(12.34)), "12.3");
        assert_eq!(format_rate(Some(1234.5)), "1234");
        assert_eq!(format_rate(Some(45_000.0)), "45.0k");
        assert_eq!(format_rate(Some(f64::NAN)), "-");
        assert_eq!(format_rate(Some(f64::INFINITY)), "-");
    }

    #[test]
    fn every_formatted_rate_fits_its_column() {
        for value in [0.0, 0.001, 9.99, 99.9, 999.9, 12_345.0, 9_999_999.0] {
            let text = format_rate(Some(value));
            assert!(
                text.chars().count() <= RPS_WIDTH as usize,
                "{text:?} overflows the rps column"
            );
        }
        for value in [0.0, 0.5, 12.5, 100.0] {
            assert!(format_percent(Some(value)).chars().count() <= ERR_WIDTH as usize);
        }
        for value in [0.0, 1.5, 99.9, 250.0, 5_000.0] {
            assert!(format_ms(Some(value)).chars().count() <= MS_WIDTH as usize);
        }
    }

    #[test]
    fn percentages_and_latencies_degrade_to_a_dash() {
        assert_eq!(format_percent(None), "-");
        assert_eq!(format_percent(Some(f64::NAN)), "-");
        assert_eq!(format_percent(Some(0.5)), "0.50%");
        assert_eq!(format_percent(Some(42.0)), "42%");

        assert_eq!(format_ms(None), "-");
        assert_eq!(format_ms(Some(1.25)), "1.2");
        assert_eq!(format_ms(Some(250.0)), "250");
        assert_eq!(format_ms(Some(2_500.0)), "2.5s");
    }

    #[test]
    fn an_error_rate_earns_colour_in_proportion_to_how_bad_it_is() {
        assert_eq!(error_style(None), Style::default());
        assert_eq!(error_style(Some(0.0)), Style::default());
        assert_eq!(error_style(Some(0.1)).fg, Some(Color::Yellow));
        assert_eq!(error_style(Some(5.0)).fg, Some(Color::Red));
        assert!(error_style(Some(5.0))
            .add_modifier
            .contains(Modifier::BOLD));
    }

    // --- key translation -------------------------------------------------

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn ordinary_keys_translate() {
        assert_eq!(translate(press(KeyCode::Char('q'))), Some(Key::Char('q')));
        assert_eq!(translate(press(KeyCode::Enter)), Some(Key::Enter));
        assert_eq!(translate(press(KeyCode::Esc)), Some(Key::Escape));
        assert_eq!(translate(press(KeyCode::Tab)), Some(Key::Tab));
        assert_eq!(translate(press(KeyCode::Up)), Some(Key::Up));
        assert_eq!(translate(press(KeyCode::Down)), Some(Key::Down));
        assert_eq!(translate(press(KeyCode::Home)), Some(Key::Home));
        assert_eq!(translate(press(KeyCode::End)), Some(Key::End));
        assert_eq!(translate(press(KeyCode::PageUp)), Some(Key::PageUp));
        assert_eq!(translate(press(KeyCode::Backspace)), Some(Key::Backspace));
    }

    #[test]
    fn ctrl_c_translates_and_other_control_chords_are_ignored() {
        let ctrl = |c| KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL);
        assert_eq!(translate(ctrl('c')), Some(Key::CtrlC));
        assert_eq!(translate(ctrl('C')), Some(Key::CtrlC));
        assert_eq!(
            translate(ctrl('l')),
            None,
            "an unbound chord must not fall through as a plain `l` and reorder the table"
        );
    }

    #[test]
    fn key_releases_are_ignored_so_windows_does_not_act_twice() {
        let mut event = press(KeyCode::Char('q'));
        event.kind = KeyEventKind::Release;
        assert_eq!(translate(event), None);

        let mut event = press(KeyCode::Char('q'));
        event.kind = KeyEventKind::Repeat;
        assert_eq!(
            translate(event),
            None,
            "a held key should not scroll on autorepeat from the terminal as well"
        );
    }

    #[test]
    fn unbound_keys_translate_to_nothing() {
        assert_eq!(translate(press(KeyCode::F(5))), None);
        assert_eq!(translate(press(KeyCode::Insert)), None);
    }

    // --- rendering -------------------------------------------------------

    fn app_with_data() -> App {
        let mut app = App::new(
            "http://127.0.0.1:10254".to_string(),
            Duration::from_secs(1),
            false,
            Instant::now(),
        );
        let start = Instant::now();
        app.record_poll(snapshot(100, None), start);
        app.record_poll(snapshot(400, None), start + Duration::from_secs(1));
        app
    }

    fn snapshot(requests: u64, pinned: Option<u64>) -> Snapshot {
        Snapshot {
            url: "http://127.0.0.1:10254".to_string(),
            generations: GenerationsResponse {
                pinned,
                serving: 42,
                generations: vec![crate::contract::GenerationEntry {
                    generation: 42,
                    applied_at: "2026-08-28T10:00:00Z".to_string(),
                    published: true,
                    digest: "a1b2c3d4e5f60718".to_string(),
                    routes: 2,
                    hosts: 2,
                    certs: 1,
                    diff: GenerationDiff {
                        summary: "1 route added".to_string(),
                        routes_added: vec!["api.example.com/v1".into()],
                        ..Default::default()
                    },
                }],
            },
            routes: RoutesResponse {
                generation: 42,
                routes: vec![
                    RouteEntry {
                        host: "api.example.com".to_string(),
                        path: "/v1".to_string(),
                        path_type: PathType::Prefix,
                        backend: "api-v2".to_string(),
                        endpoints: 4,
                        requests_total: requests,
                        errors_5xx_total: requests / 100,
                        upstream_latency_ms_sum: requests as f64 * 5.0,
                        upstream_latency_count: requests,
                        canary_stats: None,
                        canary: Some(Canary {
                            backend: "api-v3".to_string(),
                            weight_percent: 10,
                        }),
                    },
                    RouteEntry {
                        host: "*".to_string(),
                        path: "/".to_string(),
                        path_type: PathType::ImplementationSpecific,
                        backend: "default-http-backend".to_string(),
                        endpoints: 0,
                        requests_total: 7,
                        errors_5xx_total: 0,
                        upstream_latency_ms_sum: 14.0,
                        upstream_latency_count: 7,
                        canary_stats: None,
                        canary: None,
                    },
                ],
            },
            metrics: MetricsSnapshot {
                requests_total: requests,
                errors_5xx_total: requests / 100,
                active_connections: Some(37),
                generation: Some(42),
                latency_sum_seconds: requests as f64 * 0.005,
                latency_count: requests,
                pinned: None,
            },
        }
    }

    /// Renders one frame and returns the screen as text.
    fn render(app: &mut App, width: u16, height: u16) -> String {
        let mut terminal =
            Terminal::new(TestBackend::new(width, height)).expect("a test terminal");
        terminal
            .draw(|frame| draw(frame, app, Instant::now()))
            .expect("a frame");

        let buffer = terminal.backend().buffer();
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn the_routes_view_shows_the_hosts_and_their_rates() {
        let mut app = app_with_data();
        let screen = render(&mut app, 120, 20);
        assert!(screen.contains("ramjet-top"), "{screen}");
        assert!(screen.contains("api.example.com"), "{screen}");
        assert!(screen.contains("api-v2"), "{screen}");
        assert!(screen.contains("gen "), "{screen}");
        assert!(screen.contains("300"), "the per-route rate:\n{screen}");
        assert!(screen.contains("q quit"), "the footer:\n{screen}");
    }

    #[test]
    fn a_pinned_daemon_shows_the_banner() {
        let mut app = App::new(
            "http://127.0.0.1:10254".to_string(),
            Duration::from_secs(1),
            false,
            Instant::now(),
        );
        app.record_poll(snapshot(100, Some(41)), Instant::now());
        let screen = render(&mut app, 120, 20);
        assert!(screen.contains("PINNED to generation 41"), "{screen}");
        assert!(screen.contains("NOT being served"), "{screen}");
    }

    #[test]
    fn an_unpinned_daemon_gives_the_banner_row_back_to_the_table() {
        let mut app = app_with_data();
        let screen = render(&mut app, 120, 20);
        assert!(!screen.contains("PINNED"), "{screen}");
    }

    #[test]
    fn a_lost_connection_keeps_the_rows_and_says_it_is_reconnecting() {
        let mut app = app_with_data();
        app.record_failure("connection refused".to_string(), Instant::now());
        let screen = render(&mut app, 120, 20);

        assert!(
            screen.contains("api.example.com"),
            "the last good data must survive:\n{screen}"
        );
        assert!(screen.contains("reconnecting"), "{screen}");
        assert!(screen.contains("connection refused"), "{screen}");
        assert!(screen.contains("STALE"), "{screen}");
    }

    #[test]
    fn the_generations_view_lists_the_timeline_and_expands_a_diff() {
        let mut app = app_with_data();
        let now = Instant::now();
        app.on_key(Key::Tab, now);
        let screen = render(&mut app, 120, 20);
        assert!(screen.contains("generations"), "{screen}");
        assert!(screen.contains("1 route added"), "{screen}");
        assert!(screen.contains("published"), "{screen}");

        app.on_key(Key::Enter, now);
        let screen = render(&mut app, 120, 24);
        assert!(screen.contains("diff"), "{screen}");
        assert!(screen.contains("routes added (1)"), "{screen}");
        assert!(screen.contains("api.example.com/v1"), "{screen}");
    }

    #[test]
    fn a_confirmation_takes_over_the_status_line() {
        let mut app = app_with_data();
        let now = Instant::now();
        app.on_key(Key::Tab, now);
        app.on_key(Key::Down, now);
        app.on_key(Key::Char('p'), now);

        let screen = render(&mut app, 120, 20);
        assert!(screen.contains("PIN traffic to generation 42"), "{screen}");
        assert!(screen.contains("[y/N]"), "{screen}");
    }

    #[test]
    fn the_filter_box_shows_what_is_being_typed() {
        let mut app = app_with_data();
        let now = Instant::now();
        app.on_key(Key::Char('/'), now);
        app.on_key(Key::Char('a'), now);
        app.on_key(Key::Char('p'), now);

        let screen = render(&mut app, 120, 20);
        assert!(screen.contains("filter"), "{screen}");
        assert!(screen.contains("routes 1/2"), "the count narrowed:\n{screen}");
    }

    #[test]
    fn read_only_does_not_advertise_keys_it_will_refuse() {
        let mut app = App::new(
            "http://127.0.0.1:10254".to_string(),
            Duration::from_secs(1),
            true,
            Instant::now(),
        );
        app.record_poll(snapshot(100, None), Instant::now());
        app.on_key(Key::Tab, Instant::now());

        let screen = render(&mut app, 120, 20);
        assert!(screen.contains("read-only"), "{screen}");
        assert!(
            !screen.contains("p pin"),
            "a refused key must not be advertised, in the footer or the block title:\n{screen}"
        );
        assert!(!screen.contains("u unpin"), "{screen}");
    }

    #[test]
    fn drawing_survives_a_terminal_far_too_small_to_draw_in() {
        // Not a curiosity: a tiled window manager can hand a program a pane
        // this size, and every arithmetic in the layout has to survive it.
        let mut app = app_with_data();
        for (width, height) in [(1u16, 1u16), (2, 3), (10, 5), (40, 6), (200, 4)] {
            let screen = render(&mut app, width, height);
            assert_eq!(screen.lines().count(), height as usize);
        }
    }

    #[test]
    fn drawing_survives_having_nothing_to_draw() {
        let mut app = App::new(
            "http://127.0.0.1:10254".to_string(),
            Duration::from_secs(1),
            false,
            Instant::now(),
        );
        let screen = render(&mut app, 100, 20);
        assert!(screen.contains("connecting to"), "{screen}");

        app.on_key(Key::Tab, Instant::now());
        let screen = render(&mut app, 100, 20);
        assert!(screen.contains("no generations reported"), "{screen}");
    }

    #[test]
    fn a_route_with_no_endpoints_is_still_drawn() {
        let mut app = app_with_data();
        // The catch-all in the fixture has zero endpoints; it is coloured, but
        // the point here is that it is present and did not panic the styling.
        let screen = render(&mut app, 200, 20);
        assert!(screen.contains("default-http-backend"), "{screen}");
    }

    #[test]
    fn a_backend_too_long_for_its_column_is_cut_with_a_visible_mark() {
        let mut app = app_with_data();
        // On an 80-column terminal the backend column cannot hold
        // `default-http-backend`, and the cut has to be visible rather than
        // leaving a name that reads as a different, shorter backend.
        let screen = render(&mut app, 80, 20);
        assert!(!screen.contains("default-http-backend"), "{screen}");
        assert!(screen.contains('…'), "the cut is unmarked:\n{screen}");
        assert!(screen.contains("default-h"), "{screen}");
    }

    #[test]
    fn a_new_route_appears_in_the_table_without_a_rate() {
        let mut app = app_with_data();
        let mut next = snapshot(800, None);
        next.routes.routes.push(RouteEntry {
            host: "brand.new".to_string(),
            path: "/".to_string(),
            path_type: PathType::Exact,
            backend: "new-svc".to_string(),
            endpoints: 2,
            requests_total: 5,
            errors_5xx_total: 0,
            upstream_latency_ms_sum: 0.0,
            upstream_latency_count: 0,
            canary_stats: None,
            canary: None,
        });
        app.record_poll(next, Instant::now() + Duration::from_secs(2));

        let screen = render(&mut app, 120, 20);
        assert!(screen.contains("brand.new"), "{screen}");
        assert!(
            app.rows.iter().any(|r| r.host == "brand.new" && r.is_new),
            "flagged as new"
        );
    }
}
