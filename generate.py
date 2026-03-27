#!/usr/bin/env python3
"""
generate.py — Fetches live Klaviyo data and regenerates index.html.
Run via GitHub Actions on a daily schedule.
"""

import os
import sys
import json
import time
import warnings
import requests
from datetime import datetime, timedelta
from collections import defaultdict

warnings.filterwarnings("ignore")

API_KEY = os.environ.get("KLAVIYO_API_KEY", "")
if not API_KEY:
    sys.exit("Error: KLAVIYO_API_KEY environment variable not set.")

HEADERS = {
    "Authorization": f"Klaviyo-API-Key {API_KEY}",
    "revision": "2024-10-15",
}
POST_HEADERS = {**HEADERS, "Content-Type": "application/json"}
BASE = "https://a.klaviyo.com/api"

# Metric IDs for this Klaviyo account
M_OPENED   = "YzDDK5"   # Opened Email
M_CLICKED  = "WWugVn"   # Clicked Email
M_RECEIVED = "UmLQ7u"   # Received Email (delivered)
M_UNSUB    = "RqnkXf"   # Unsubscribed from Email Marketing
M_BOUNCED  = "THG3pB"   # Bounced Email
M_ORDER    = "UBEB9E"   # Placed Order

EMAIL_LIST_ID = "UyzQTT"  # "Email List" — newsletter subscribers

# Campaign grouping rules (order matters — first match wins)
CAMPAIGN_GROUPS = [
    ("Newsletters",        ["newsletter", "jb note"]),
    ("Price Change",       ["price change"]),
    ("Early Bird",         ["early bird"]),
    ("Beta Launch",        ["beta", "5.16"]),
    ("Fit Confirmation",   ["fit confirmation", "fit email"]),
    ("Subscription",       ["subscription"]),
    ("Cloned Campaigns",   ["clone"]),
]


# ─── API helpers ──────────────────────────────────────────────────────────────

def _get_with_retry(url, headers, params=None, retries=5):
    for attempt in range(retries):
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 429:
            wait = 2 ** attempt
            print(f"    Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r

def api_get(path, params=None):
    r = _get_with_retry(f"{BASE}{path}", HEADERS, params)
    return r.json()

def api_get_all(path, params=None):
    results, url = [], f"{BASE}{path}"
    while url:
        r = _get_with_retry(url, HEADERS, params)
        d = r.json()
        results.extend(d.get("data", []))
        url = d.get("links", {}).get("next")
        params = None
    return results

def metric_agg_by(metric_id, by_field, days=365):
    """Aggregate a metric grouped by a dimension. Returns {name: total_unique}."""
    end = datetime.utcnow()
    start = end - timedelta(days=days)
    payload = {"data": {"type": "metric-aggregate", "attributes": {
        "metric_id": metric_id,
        "measurements": ["unique"],
        "by": [by_field],
        "filter": [
            f"greater-or-equal(datetime,{start.strftime('%Y-%m-%dT%H:%M:%S')})",
            f"less-than(datetime,{end.strftime('%Y-%m-%dT%H:%M:%S')})",
        ],
        "interval": "month",
    }}}
    r = requests.post(f"{BASE}/metric-aggregates/", headers=POST_HEADERS, json=payload)
    if r.status_code == 429:
        time.sleep(2)
        r = requests.post(f"{BASE}/metric-aggregates/", headers=POST_HEADERS, json=payload)
    if r.status_code != 200:
        print(f"  Warning: metric_agg_by({metric_id}, {by_field}) failed: {r.status_code}")
        return {}
    data = r.json().get("data", {}).get("attributes", {}).get("data", [])
    result = {}
    for item in data:
        name = item["dimensions"][0] if item["dimensions"] else ""
        if name:
            result[name] = result.get(name, 0) + sum(item["measurements"]["unique"])
    return result

def metric_total_unique(metric_id, days=365):
    """Get total unique count for a metric (no grouping)."""
    end = datetime.utcnow()
    start = end - timedelta(days=days)
    payload = {"data": {"type": "metric-aggregate", "attributes": {
        "metric_id": metric_id,
        "measurements": ["unique"],
        "filter": [
            f"greater-or-equal(datetime,{start.strftime('%Y-%m-%dT%H:%M:%S')})",
            f"less-than(datetime,{end.strftime('%Y-%m-%dT%H:%M:%S')})",
        ],
        "interval": "month",
    }}}
    r = requests.post(f"{BASE}/metric-aggregates/", headers=POST_HEADERS, json=payload)
    if r.status_code != 200:
        return 0
    data = r.json().get("data", {}).get("attributes", {}).get("data", [])
    if data:
        return int(sum(data[0]["measurements"]["unique"]))
    return 0

def list_profile_count(list_id):
    """Count profiles in a Klaviyo list by paginating."""
    url, count = f"{BASE}/lists/{list_id}/profiles/", 0
    params = {"page[size]": 100, "fields[profile]": "id"}
    while url:
        r = requests.get(url, headers=HEADERS, params=params)
        if r.status_code != 200:
            return None
        d = r.json()
        count += len(d.get("data", []))
        url = d.get("links", {}).get("next")
        params = None
    return count


# ─── Data processing ──────────────────────────────────────────────────────────

def pct(n, d):
    """Format n/d as a percentage string."""
    if not d:
        return "—"
    return f"{100 * n / d:.1f}%"

def classify_campaign(name):
    """Assign a campaign to a group based on name keywords."""
    n = name.lower()
    for group, keywords in CAMPAIGN_GROUPS:
        if any(k in n for k in keywords):
            return group
    return "Other"

def fmt_date(dt_str):
    """Format an ISO datetime string to YYYY-MM-DD."""
    if not dt_str:
        return "—"
    try:
        return dt_str[:10]
    except Exception:
        return dt_str


# ─── HTML generation helpers ──────────────────────────────────────────────────

def bar_html(pct_str, cls=""):
    try:
        v = float(pct_str.rstrip("%"))
    except (ValueError, AttributeError):
        v = 0
    w = min(100, v)
    c = f" {cls}" if cls else ""
    return f'<div class="bar-container"><div class="bar{c}" style="width:{w:.0f}%"></div></div>'

CSS = """
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #FFFFFF;
    color: #363A4F;
    padding: 40px;
    max-width: 1200px;
    margin: 0 auto;
  }
  h1 { font-size: 28px; color: #2B2E3F; margin-bottom: 8px; }
  .subtitle { color: #363A4F; opacity: 0.7; margin-bottom: 32px; font-size: 14px; }

  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 16px; margin-bottom: 40px;
  }
  .kpi-card {
    background: #C1CFF822;
    border-left: 4px solid #AAA8FF;
    border-radius: 8px; padding: 20px;
  }
  .kpi-card .label {
    font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;
    color: #363A4F; opacity: 0.7; margin-bottom: 6px;
  }
  .kpi-card .value { font-size: 28px; font-weight: 700; color: #2B2E3F; }
  .kpi-card .detail { font-size: 12px; color: #363A4F; opacity: 0.6; margin-top: 4px; }

  .section { margin-bottom: 40px; }
  .section h2 {
    font-size: 20px; color: #2B2E3F; margin-bottom: 8px;
    padding-bottom: 8px; border-bottom: 2px solid #C1CFF8;
  }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th {
    text-align: left; padding: 10px 12px;
    background: #C1CFF833; color: #2B2E3F;
    font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px;
  }
  td { padding: 10px 12px; border-bottom: 1px solid #C1CFF844; }
  tr:hover td { background: #FFFBA544; }
  .audience-cell { font-size: 11px; color: #363A4F; opacity: 0.8; max-width: 200px; }

  .bar-container {
    width: 80px; height: 8px; background: #C1CFF833;
    border-radius: 4px; display: inline-block; vertical-align: middle; margin-right: 8px;
  }
  .bar { height: 100%; border-radius: 4px; background: #AAA8FF; }
  .bar.click { background: #7F9EF8; }

  .group-header td {
    background: #C1CFF818; font-weight: 700; font-size: 13px;
    color: #2B2E3F; padding: 14px 12px 8px;
    border-bottom: 2px solid #C1CFF866;
  }

  .callout {
    background: #FFFBA566;
    border-radius: 10px;
    padding: 20px 24px;
    margin-bottom: 32px;
  }
  .callout h3 {
    font-size: 14px; text-transform: uppercase; letter-spacing: 0.5px;
    color: #2B2E3F; margin-bottom: 12px;
  }
  .callout ul { list-style: none; padding: 0; margin: 0; }
  .callout li {
    padding: 6px 0; font-size: 13px; color: #363A4F;
    border-bottom: 1px solid #FFFBA5;
  }
  .callout li:last-child { border-bottom: none; }
  .callout .date-tag {
    display: inline-block; font-size: 11px; font-weight: 600;
    color: #363A4F; opacity: 0.6; min-width: 50px; margin-right: 8px;
  }
  .callout .stat-tag { font-size: 11px; color: #363A4F; opacity: 0.6; margin-left: 6px; }
  .callout .draft-badge {
    display: inline-block; padding: 1px 7px; border-radius: 8px;
    font-size: 10px; font-weight: 600;
    background: #FFC28A44; color: #363A4F; margin-left: 6px;
  }
  .callout .scheduled-badge {
    display: inline-block; padding: 1px 7px; border-radius: 8px;
    font-size: 10px; font-weight: 600;
    background: #C1CFF866; color: #363A4F; margin-left: 6px;
  }

  .status-badge {
    display: inline-block; padding: 2px 8px; border-radius: 10px;
    font-size: 11px; font-weight: 600;
  }
  .status-live   { background: #71D68844; color: #2d7a3e; }
  .status-draft  { background: #FFFBA5; color: #363A4F; }
  .status-manual { background: #C1CFF844; color: #363A4F; }

  .flow-group {
    margin-bottom: 24px; border: 1px solid #C1CFF844;
    border-radius: 8px; overflow: hidden;
  }
  .flow-group-header {
    background: #C1CFF822; padding: 12px 16px;
    font-weight: 600; font-size: 14px; color: #2B2E3F;
    display: flex; justify-content: space-between; align-items: center;
  }
  .flow-group table { margin: 0; }
  .flow-group-meta {
    padding: 0 16px 12px; background: #C1CFF822;
    font-size: 12px; color: #363A4F; opacity: 0.8;
  }
  .flow-group-meta span { margin-right: 20px; }
"""


def build_html(generated_at, kpis, campaigns, flows, drafts_scheduled, lists_map):
    sent_campaigns = [c for c in campaigns if c["status"] in ("Sent", "Sending")]
    today = datetime.utcnow().date()
    cutoff = today - timedelta(days=14)

    # ── Recent updates ──
    recent_rows = []
    for c in sorted(sent_campaigns, key=lambda x: x["send_time"] or "", reverse=True):
        send_date_str = (c["send_time"] or "")[:10]
        if not send_date_str or send_date_str < str(cutoff):
            break
        recipients = c["recipients"]
        open_rate = c["open_rate"]
        stat = ""
        if recipients:
            stat = f' <span class="stat-tag">sent to {recipients:,} &mdash; {open_rate} open rate</span>'
        recent_rows.append(f'<li><span class="date-tag">{send_date_str}</span> {c["name"]}{stat}</li>')

    for c in drafts_scheduled:
        badge_cls = "draft-badge" if c["status"] == "Draft" else "scheduled-badge"
        recent_rows.append(
            f'<li><span class="date-tag">{fmt_date(c["created_at"])}</span> '
            f'<strong>{c["name"]}</strong> <span class="{badge_cls}">{c["status"].lower()}</span></li>'
        )

    recent_html = "\n    ".join(recent_rows) if recent_rows else "<li>No recent campaigns</li>"

    # ── Campaign table ──
    grouped = defaultdict(list)
    for c in sent_campaigns:
        grouped[c["group"]].append(c)

    group_order = [g for g, _ in CAMPAIGN_GROUPS] + ["Other"]
    campaign_rows = []
    for group in group_order:
        cs = grouped.get(group, [])
        if not cs:
            continue
        campaign_rows.append(f'<tr class="group-header"><td colspan="9">{group}</td></tr>')
        for c in sorted(cs, key=lambda x: x["send_time"] or "", reverse=True):
            audience = c["audience"]
            recipients = f'{c["recipients"]:,}' if c["recipients"] else "—"
            open_rate = c["open_rate"]
            click_rate = c["click_rate"]
            delivery = c["delivery"]
            unsubs = c["unsubs"] if c["unsubs"] is not None else "—"
            campaign_rows.append(f"""      <tr>
        <td>{c['name']}</td>
        <td class="audience-cell">{audience}</td>
        <td>{recipients}</td>
        <td>{open_rate}</td>
        <td>{bar_html(open_rate)}</td>
        <td>{click_rate}</td>
        <td>{bar_html(click_rate, 'click')}</td>
        <td>{delivery}</td>
        <td>{unsubs}</td>
      </tr>""")

    campaign_table = "\n".join(campaign_rows)

    # ── Flows ──
    flow_sections = []
    for flow in flows:
        status_cls = {"live": "status-live", "draft": "status-draft", "manual": "status-manual"}.get(
            flow["status"].lower(), "status-draft"
        )
        msg_rows = []
        for msg in flow["messages"]:
            r = msg["recipients"]
            recipients_str = f"{r:,}" if r else "—"
            msg_rows.append(f"""        <tr>
          <td><strong>{msg['name']}</strong></td>
          <td>{recipients_str}</td>
          <td>{msg['open_rate']}</td>
          <td>{bar_html(msg['open_rate'])}</td>
          <td>{msg['click_rate']}</td>
          <td>{bar_html(msg['click_rate'], 'click')}</td>
          <td>{msg['delivery']}</td>
          <td>{msg['unsubs'] if msg['unsubs'] is not None else '—'}</td>
        </tr>""")
        msgs_html = "\n".join(msg_rows) or "        <tr><td colspan='8' style='opacity:.5'>No messages</td></tr>"
        flow_sections.append(f"""  <div class="flow-group">
    <div class="flow-group-header">
      <span>{flow['name']}</span>
      <span class="status-badge {status_cls}">{flow['status'].lower()}</span>
    </div>
    <div class="flow-group-meta">
      <span>Trigger: <strong>{flow['trigger_type']}</strong></span>
    </div>
    <table>
      <thead><tr>
        <th>Message</th><th>Recipients</th>
        <th>Open Rate</th><th></th>
        <th>Click Rate</th><th></th>
        <th>Delivery</th><th>Unsubs</th>
      </tr></thead>
      <tbody>
{msgs_html}
      </tbody>
    </table>
  </div>""")
    flows_html = "\n".join(flow_sections) if flow_sections else '<p class="empty-state">No active flows found.</p>'

    # ── Drafts & scheduled table ──
    draft_rows = []
    for c in drafts_scheduled:
        send_time = fmt_date(c.get("send_time") or c.get("scheduled_at") or "—")
        draft_rows.append(
            f'    <tr><td>{c["name"]}</td><td>{c["status"]}</td>'
            f'<td>{fmt_date(c["created_at"])}</td><td>{send_time}</td></tr>'
        )
    drafts_table = "\n".join(draft_rows) if draft_rows else '    <tr><td colspan="4" style="opacity:.5">None</td></tr>'

    # ── KPI cards ──
    nl_subs = kpis["newsletter_subscribers"]
    purchasers = kpis["purchasers"]
    avg_open = kpis["avg_open_rate"]
    avg_click = kpis["avg_click_rate"]
    sent_count = kpis["sent_count"]

    nl_value = f"{nl_subs:,}" if nl_subs else "—"
    purch_value = f"{purchasers:,}" if purchasers else "—"
    open_value = f"{avg_open:.1f}%" if avg_open else "—"
    click_value = f"{avg_click:.1f}%" if avg_click else "—"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NextSense — Klaviyo Email Dashboard</title>
<style>
{CSS}
</style>
</head>
<body>

<h1>Klaviyo Email Dashboard</h1>
<p class="subtitle">Generated {generated_at} &nbsp;|&nbsp; NextSense Smartbuds &nbsp;|&nbsp; Live data via Klaviyo API</p>

<div class="kpi-grid">
  <div class="kpi-card">
    <div class="label">Newsletter Subscribers</div>
    <div class="value">{nl_value}</div>
    <div class="detail">Email List</div>
  </div>
  <div class="kpi-card">
    <div class="label">Purchasers</div>
    <div class="value">{purch_value}</div>
    <div class="detail">Placed Order (last 365d)</div>
  </div>
  <div class="kpi-card">
    <div class="label">Avg. Open Rate</div>
    <div class="value">{open_value}</div>
    <div class="detail">Across {sent_count} sent campaigns</div>
  </div>
  <div class="kpi-card">
    <div class="label">Avg. Click Rate</div>
    <div class="value">{click_value}</div>
  </div>
</div>

<div class="callout">
  <h3>Recent Updates</h3>
  <ul>
    {recent_html}
  </ul>
</div>

<!-- Campaign Performance -->
<div class="section">
  <h2>Campaign Performance</h2>
  <table>
    <thead><tr>
      <th>Campaign</th>
      <th>Audience</th>
      <th>Recipients</th>
      <th>Open Rate</th><th></th>
      <th>Click Rate</th><th></th>
      <th>Delivery</th>
      <th>Unsubs</th>
    </tr></thead>
    <tbody>
{campaign_table}
    </tbody>
  </table>
</div>

<!-- Flow Performance -->
<div class="section">
  <h2>Flow Performance</h2>
{flows_html}
</div>

<div class="section">
  <h2>Scheduled &amp; Draft Campaigns</h2>
  <table><thead><tr><th>Campaign</th><th>Status</th><th>Created</th><th>Send Time</th></tr></thead><tbody>
{drafts_table}
  </tbody></table>
</div>

</body>
</html>
"""


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Fetching Klaviyo data...")
    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # 1. Fetch all lists for audience name lookup
    print("  • Lists...")
    all_lists = api_get_all("/lists/")
    lists_map = {lst["id"]: lst["attributes"]["name"] for lst in all_lists}

    # 2. Newsletter subscriber count
    print("  • Newsletter subscriber count...")
    nl_subs = list_profile_count(EMAIL_LIST_ID)

    # 3. Purchasers (unique Placed Order events, last 365 days)
    print("  • Purchasers...")
    purchasers = metric_total_unique(M_ORDER, days=365)

    # 4. Campaign-level metric aggregates (grouped by Campaign Name)
    print("  • Campaign metrics (opens, clicks, received, unsubs, bounced)...")
    camp_opens    = metric_agg_by(M_OPENED,   "Campaign Name")
    camp_clicks   = metric_agg_by(M_CLICKED,  "Campaign Name")
    camp_received = metric_agg_by(M_RECEIVED, "Campaign Name")
    camp_unsubs   = metric_agg_by(M_UNSUB,    "Campaign Name")
    camp_bounced  = metric_agg_by(M_BOUNCED,  "Campaign Name")

    # 5. Flow message-level metric aggregates (grouped by Flow Message Name)
    print("  • Flow message metrics...")
    msg_opens    = metric_agg_by(M_OPENED,   "Flow Message Name")
    msg_clicks   = metric_agg_by(M_CLICKED,  "Flow Message Name")
    msg_received = metric_agg_by(M_RECEIVED, "Flow Message Name")
    msg_unsubs   = metric_agg_by(M_UNSUB,    "Flow Message Name")
    msg_bounced  = metric_agg_by(M_BOUNCED,  "Flow Message Name")

    # 6. Campaigns
    print("  • Campaigns...")
    raw_campaigns = api_get_all("/campaigns/", params={
        "filter": "equals(messages.channel,'email')",
        "sort": "-created_at",
    })

    campaigns = []
    for c in raw_campaigns:
        attrs = c["attributes"]
        name = attrs["name"]
        status = attrs["status"]
        send_time = attrs.get("send_time") or attrs.get("scheduled_at")
        created_at = attrs.get("created_at")
        audience_ids = attrs.get("audiences", {}).get("included", [])
        audience = ", ".join([lists_map.get(lid, lid) for lid in audience_ids[:3]])
        if len(audience_ids) > 3:
            audience += " ..."

        received = int(camp_received.get(name, 0))
        opened   = int(camp_opens.get(name, 0))
        clicked  = int(camp_clicks.get(name, 0))
        unsubs   = int(camp_unsubs.get(name, 0))
        bounced  = int(camp_bounced.get(name, 0))
        total_sent = received + bounced

        campaigns.append({
            "id":          c["id"],
            "name":        name,
            "status":      status,
            "group":       classify_campaign(name),
            "send_time":   send_time,
            "created_at":  created_at,
            "scheduled_at": attrs.get("scheduled_at"),
            "audience":    audience,
            "recipients":  received,
            "open_rate":   pct(opened, received),
            "click_rate":  pct(clicked, received),
            "delivery":    pct(received, total_sent) if total_sent else "—",
            "unsubs":      unsubs,
        })

    sent_campaigns = [c for c in campaigns if c["status"] in ("Sent", "Sending")]
    drafts_scheduled = [c for c in campaigns if c["status"] in ("Draft", "Scheduled")]

    # 7. KPIs
    open_rates  = [float(c["open_rate"].rstrip("%"))  for c in sent_campaigns if c["open_rate"] != "—"]
    click_rates = [float(c["click_rate"].rstrip("%")) for c in sent_campaigns if c["click_rate"] != "—"]

    kpis = {
        "newsletter_subscribers": nl_subs,
        "purchasers":             purchasers,
        "avg_open_rate":          sum(open_rates) / len(open_rates)   if open_rates  else None,
        "avg_click_rate":         sum(click_rates) / len(click_rates) if click_rates else None,
        "sent_count":             len(sent_campaigns),
    }

    # 8. Flows
    print("  • Flows...")
    raw_flows = api_get_all("/flows/", params={"filter": "equals(archived,false)"})
    flows = []
    for flow in raw_flows:
        attrs = flow["attributes"]
        status = attrs["status"]
        if status == "draft":
            continue

        flow_id   = flow["id"]
        flow_name = attrs["name"]
        trigger   = attrs.get("trigger_type", "Unknown")

        # Get SEND_EMAIL actions for this flow
        actions = api_get_all(f"/flows/{flow_id}/flow-actions/",
                              params={"filter": "equals(action_type,'SEND_EMAIL')"})

        messages = []
        for action in actions:
            msgs = api_get_all(f"/flow-actions/{action['id']}/flow-messages/")
            for msg in msgs:
                msg_name  = msg["attributes"]["name"]
                received  = int(msg_received.get(msg_name, 0))
                opened    = int(msg_opens.get(msg_name, 0))
                clicked   = int(msg_clicks.get(msg_name, 0))
                unsubs    = int(msg_unsubs.get(msg_name, 0))
                bounced   = int(msg_bounced.get(msg_name, 0))
                total_s   = received + bounced
                messages.append({
                    "name":       msg_name,
                    "recipients": received,
                    "open_rate":  pct(opened, received),
                    "click_rate": pct(clicked, received),
                    "delivery":   pct(received, total_s) if total_s else "—",
                    "unsubs":     unsubs,
                })

        if messages:
            flows.append({
                "name":         flow_name,
                "status":       status,
                "trigger_type": trigger,
                "messages":     messages,
            })

    # 9. Generate HTML
    print("  • Generating HTML...")
    html = build_html(generated_at, kpis, campaigns, flows, drafts_scheduled, lists_map)

    output_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Done! index.html written ({len(html):,} bytes)")
    print(f"  Campaigns: {len(sent_campaigns)} sent, {len(drafts_scheduled)} draft/scheduled")
    print(f"  Flows: {len(flows)} active")
    print(f"  Newsletter subscribers: {nl_subs}")
    print(f"  Purchasers (365d): {purchasers}")


if __name__ == "__main__":
    main()
