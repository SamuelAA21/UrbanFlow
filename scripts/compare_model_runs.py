"""Compare real-label forecasts on identical date/zone/hour keys, not global cohorts."""
import argparse
import csv
import json
import math
from pathlib import Path


def read(path):
    with Path(path).open(encoding='utf-8',newline='') as source:
        return {(r['fecha'],r['zona_origen'],r['hora']):(float(r['viajes']),float(r['pred_modelo']))
                for r in csv.DictReader(source)}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('before')
    parser.add_argument('after')
    args=parser.parse_args()
    before,after=read(args.before),read(args.after)
    keys=sorted(set(before)&set(after))
    if not keys or any(before[k][0]!=after[k][0] for k in keys):
        raise SystemExit('No common independent labels, or source labels changed.')
    result={'common_real_cells':len(keys),'test_from':min(k[0] for k in keys),'test_until':max(k[0] for k in keys),
            'before_total_cells':len(before),'after_total_cells':len(after),'synthetic_labels':0}
    for name,values in (('before',before),('after',after)):
        errors=[values[k][1]-values[k][0] for k in keys]
        result[name]={'mae':sum(abs(e) for e in errors)/len(keys),'rmse':math.sqrt(sum(e*e for e in errors)/len(keys))}
    result['mae_reduction_percent']=100*(result['before']['mae']-result['after']['mae'])/result['before']['mae']
    result['interpretation']='Only this matched historical test; not a guarantee of future improvement.'
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
