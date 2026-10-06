"""Evidencia de primera entrega. Python estándar; no implementa el pipeline Spark."""
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
import argparse
import csv
import hashlib
import json
import math

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / 'data/datalake/landing'
KEYS = {'customers_orgs': ['org_id'], 'users': ['user_id'],
        'resources': ['resource_id'], 'support_tickets': ['ticket_id'],
        'marketing_touches': ['touch_id'], 'nps_surveys': ['org_id', 'survey_date'],
        'billing_monthly': ['invoice_id']}

def load(landing=LANDING):
    tables = {}
    for name in KEYS:
        with (landing / f'{name}.csv').open(encoding='utf-8', newline='') as f:
            tables[name] = list(csv.DictReader(f))
    batches = []
    for file in sorted((landing / 'usage_events_stream').glob('*.jsonl')):
        with file.open(encoding='utf-8') as f:
            batches.append((file.name, [json.loads(line) for line in f if line.strip()]))
    return tables, batches

def num(value):
    if value is None or value == '':
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None

def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))

def quality(tables, batches):
    ev = [e for _, rows in batches for e in rows]
    orgs = {r['org_id']: r for r in tables['customers_orgs']}
    resources = {r['resource_id']: r for r in tables['resources']}
    issues = []
    def add(source, rule, count, total, treatment):
        issues.append(dict(source=source, rule=rule, count=count, total=total,
                           percent=round(count * 100 / total, 3), treatment=treatment))
    for name, rows in tables.items():
        keys = [tuple(r[k] for k in KEYS[name]) for r in rows]
        add(name, 'duplicate_key', len(keys)-len(set(keys)), len(rows), 'dedupe')
        if name != 'customers_orgs':
            add(name, 'orphan_org', sum(r['org_id'] not in orgs for r in rows), len(rows), 'quarantine')
    n = len(ev)
    add('usage_events', 'duplicate_event_id', n-len({e['event_id'] for e in ev}), n, 'dedupe')
    add('usage_events', 'value_string', sum(isinstance(e['value'], str) for e in ev), n, 'cast_controlado')
    add('usage_events', 'value_null', sum(e['value'] is None for e in ev), n, 'conservar_nulo_y_contar')
    add('usage_events', 'value_invalid_cast', sum(e['value'] is not None and num(e['value']) is None for e in ev), n, 'quarantine_metrica')
    add('usage_events', 'unit_null', sum(e['unit'] is None for e in ev), n, 'inferir_desde_metric_con_flag')
    add('usage_events', 'negative_cost', sum(e['cost_usd_increment'] < 0 for e in ev), n, 'flag_sin_borrar')
    add('usage_events', 'cost_gt_100_usd', sum(e['cost_usd_increment'] > 100 for e in ev), n, 'indicador_exploratorio_no_umbral_final')
    add('usage_events', 'orphan_org', sum(e['org_id'] not in orgs for e in ev), n, 'quarantine')
    add('usage_events', 'orphan_resource', sum(e['resource_id'] not in resources for e in ev), n, 'quarantine')
    add('usage_events', 'resource_org_mismatch', sum(e['resource_id'] in resources and e['org_id'] != resources[e['resource_id']]['org_id'] for e in ev), n, 'quarantine')
    add('usage_events', 'resource_service_mismatch', sum(e['resource_id'] in resources and e['service'] != resources[e['resource_id']]['service'] for e in ev), n, 'flag')
    tags = [r['tags_json'] for r in tables['resources'] if r['tags_json']]
    invalid_tags = 0
    for value in tags:
        try:
            if not isinstance(json.loads(value), list):
                invalid_tags += 1
        except json.JSONDecodeError:
            invalid_tags += 1
    add('resources', 'invalid_nonempty_tags_json', invalid_tags, len(tags), 'validar_parser_csv_antes_de_reparar')
    add('resources', 'tags_null', len(resources)-len(tags), len(resources), 'preservar_ausencia')
    bills = tables['billing_monthly']
    add('billing_monthly', 'negative_subtotal', sum(num(r['subtotal']) < 0 for r in bills), len(bills), 'flag_pendiente_regla_negocio')
    add('billing_monthly', 'credits_null', sum(num(r['credits']) is None for r in bills), len(bills), 'cero_con_flag_supuesto')
    add('billing_monthly', 'usd_fx_not_one', sum(r['currency'] == 'USD' and num(r['exchange_rate_to_usd']) != 1 for r in bills), len(bills), 'fx_efectivo_uno_con_flag')
    users = tables['users']
    add('users', 'login_before_created', sum(bool(r['last_login']) and r['last_login'] < r['created_at'] for r in users), len(users), 'flag')
    for name in ['customers_orgs', 'nps_surveys']:
        rows = tables[name]
        add(name, 'nps_null', sum(num(r['nps_score']) is None for r in rows), len(rows), 'conservar_nulo')
        add(name, 'nps_outside_minus100_100', sum(num(r['nps_score']) is not None and not -100 <= num(r['nps_score']) <= 100 for r in rows), len(rows), 'flag_escala_a_confirmar')
    tickets = tables['support_tickets']
    add('support_tickets', 'open_ticket', sum(not r['resolved_at'] for r in tickets), len(tickets), 'estado_valido')
    add('support_tickets', 'csat_null', sum(num(r['csat']) is None for r in tickets), len(tickets), 'denominador_solo_validos')
    add('support_tickets', 'csat_outside_1_5', sum(num(r['csat']) is not None and not 1 <= num(r['csat']) <= 5 for r in tickets), len(tickets), 'flag_escala_a_confirmar')
    return issues

def watermark_simulation(batches):
    """Orden lexicográfico, un archivo/lote; umbral = máximo de lotes previos - margen.

    Es una estimación de elegibilidad temporal, NO una corrida de Spark ni una
    medición del descarte real. No modela la latencia de actualización del watermark.
    """
    result = []
    total = sum(len(rows) for _, rows in batches)
    for hours in [1, 2, 24, 168, 720, 1440]:
        maximum, late = None, 0
        for _, rows in batches:
            times = [timestamp(r['timestamp']) for r in rows]
            if maximum is not None:
                threshold = maximum - timedelta(hours=hours)
                late += sum(t < threshold for t in times)
            maximum = max(times + ([maximum] if maximum else []))
        result.append(dict(margin_hours=hours, below_threshold=late,
                           total=total, percent=round(100*late/total, 3)))
    return result

def mapped(event):
    key = (event['org_id'], event['timestamp'][:10], event['service'])
    value = num(event['value'])
    # Decimal evita error acumulado de punto flotante en importes.
    cost = Decimal(str(event['cost_usd_increment']))
    metrics = [Decimal(str(value)) if event['metric'] == metric and value is not None
               else Decimal(0) for metric in ['requests', 'cpu_hours', 'storage_gb_hours']]
    return key, [cost, *metrics, 1, int(value is None), int(cost < 0)]

def mapreduce(batches):
    reduced = {}
    mapped_pairs, combined_pairs = 0, 0
    for _, rows in batches:
        combined = {}
        for event in rows:
            key, value = mapped(event)
            mapped_pairs += 1
            if key not in combined:
                combined[key] = [Decimal(0)] * 4 + [0] * 3
            combined[key] = [a+b for a, b in zip(combined[key], value)]
        combined_pairs += len(combined)
        for key, value in combined.items():
            if key not in reduced:
                reduced[key] = [Decimal(0)] * 4 + [0] * 3
            reduced[key] = [a+b for a, b in zip(reduced[key], value)]
    output = []
    fields = ['daily_cost_usd', 'requests_known', 'cpu_hours_known',
              'storage_gb_hours_known', 'event_count', 'missing_value_count', 'negative_cost_count']
    for key, values in sorted(reduced.items()):
        output.append(dict(zip(['org_id', 'usage_date', 'service', *fields],
                               [*key, *[str(v) for v in values]])))
    # Verificación independiente: ordenar y recorrer grupos sin combiner.
    direct = defaultdict(lambda: Decimal(0))
    for _, rows in batches:
        for event in rows:
            key = (event['org_id'], event['timestamp'][:10], event['service'])
            direct[key] += Decimal(str(event['cost_usd_increment']))
    assert len(direct) == len(output)
    assert all(direct[(r['org_id'], r['usage_date'], r['service'])] == Decimal(r['daily_cost_usd']) for r in output)
    assert sum(int(r['event_count']) for r in output) == mapped_pairs
    return output, dict(mapped_pairs=mapped_pairs, combined_pairs=combined_pairs,
                        gold_keys=len(output), total_cost_usd=str(sum(direct.values())),
                        validation='cost_by_key_and_event_count_ok')

def write_csv(path, rows):
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

def run(landing=LANDING, output=ROOT/'evidence'):
    output.mkdir(parents=True, exist_ok=True)
    tables, batches = load(landing)
    events = [e for _, rows in batches for e in rows]
    inventory = []
    for name, rows in tables.items():
        inventory.append(dict(source=name, rows=len(rows), columns=len(rows[0]),
                              key='+'.join(KEYS[name]), mode='batch'))
    inventory.append(dict(source='usage_events', rows=len(events), columns=len(set().union(*(r.keys() for r in events))), key='event_id', mode='streaming_simulado'))
    file_profile = [dict(file=name, rows=len(rows),
                         min_timestamp=min(r['timestamp'] for r in rows),
                         max_timestamp=max(r['timestamp'] for r in rows),
                         distinct_dates=len({r['timestamp'][:10] for r in rows})) for name, rows in batches]
    mart, mr = mapreduce(batches)
    version = Counter(r['schema_version'] for r in events)
    summary = dict(event_count=len(events), file_count=len(batches),
                   date_min=min(r['timestamp'] for r in events), date_max=max(r['timestamp'] for r in events),
                   event_dates=len({r['timestamp'][:10] for r in events}),
                   schema_versions=dict(version), value_types=dict(Counter(type(r['value']).__name__ for r in events)),
                   metric_units={m: sorted({r['unit'] for r in events if r['metric']==m and r['unit'] is not None}) for m in sorted({r['metric'] for r in events})},
                   mapreduce=mr)
    for name, rows in [('inventory', inventory), ('quality', quality(tables,batches)),
                       ('watermark_simulation', watermark_simulation(batches)),
                       ('file_time_profile', file_profile), ('usage_daily_reference', mart)]:
        write_csv(output/f'{name}.csv', rows)
    (output/'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False)+'\n', encoding='utf-8')
    manifest = {str(p.relative_to(landing)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(landing.rglob('*')) if p.is_file()}
    (output/'landing_sha256.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return summary

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--landing', type=Path, default=LANDING)
    parser.add_argument('--output', type=Path, default=ROOT/'evidence')
    args = parser.parse_args()
    print(json.dumps(run(args.landing, args.output), indent=2, ensure_ascii=False))
