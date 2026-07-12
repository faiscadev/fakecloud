#!/usr/bin/env python3
"""Regenerate crates/fakecloud-iot/src/generated.rs from aws-models/iot.json.

The AWS IoT Core control plane has 272 operations; hand-maintaining the route
table, per-operation HTTP bindings, model-derived input constraints, and output
member shapes would be error-prone, so they are generated directly from the
Smithy model. Run after refreshing aws-models/iot.json:

    python3 scripts/generate-iot-tables.py
"""
import json, os
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---- metadata builder ----
import json
M=os.path.join(REPO,"aws-models","iot.json")
d=json.load(open(M)); shapes=d['shapes']
NS="com.amazonaws.iot#"
def sid(name): return NS+name
def get(name):
    return shapes.get(NS+name)
def sn(t): return t.split('#')[1]
def resolve(tid):
    return shapes.get(tid)
def kind_of(tid):
    s=resolve(tid)
    if not s:
        # prelude
        p=tid.split('#')[1] if '#' in tid else tid
        if p in ('String','Document'): return 'Str'
        if p in ('Integer','Long','Short','Byte','BigInteger','PrimitiveInteger','PrimitiveLong'): return 'Int'
        if p in ('Float','Double','BigDecimal','PrimitiveFloat','PrimitiveDouble'): return 'Num'
        if p in ('Boolean','PrimitiveBoolean'): return 'Bool'
        if p=='Blob': return 'Blob'
        if p=='Timestamp': return 'Ts'
        return 'Str'
    t=s['type']
    if t=='structure': return 'Struct'
    if t=='list': return 'List'
    if t=='map': return 'Map'
    if t=='union': return 'Struct'
    if t=='enum': return 'Str'
    if t in ('string',): return 'Str'
    if t in ('integer','long','short','byte','biginteger'): return 'Int'
    if t in ('float','double','bigdecimal'): return 'Num'
    if t=='boolean': return 'Bool'
    if t=='blob': return 'Blob'
    if t=='timestamp': return 'Ts'
    if t=='document': return 'Str'
    return 'Str'
def wire_name(mname, m):
    return m.get('traits',{}).get('smithy.api#jsonName', mname)
def enum_values(tid):
    s=resolve(tid)
    if not s: return None
    if s['type']=='enum':
        vals=[]
        for mn,mm in s.get('members',{}).items():
            ev=mm.get('traits',{}).get('smithy.api#enumValue', mn)
            vals.append(ev)
        return vals
    # old string enum
    et=s.get('traits',{}).get('smithy.api#enum')
    if et:
        return [e['value'] for e in et]
    return None
def constraints(tid, mtr):
    s=resolve(tid)
    st=(s or {}).get('traits',{})
    def pick(k):
        if k in mtr: return mtr[k]
        return st.get(k)
    ln=pick('smithy.api#length') or {}
    rg=pick('smithy.api#range') or {}
    return ln.get('min'),ln.get('max'),rg.get('min'),rg.get('max')
def binding(mtr):
    if 'smithy.api#httpLabel' in mtr: return 'label'
    if 'smithy.api#httpQuery' in mtr: return 'query'
    if 'smithy.api#httpQueryParams' in mtr: return 'queryparams'
    if 'smithy.api#httpHeader' in mtr: return 'header'
    if 'smithy.api#httpPrefixHeaders' in mtr: return 'prefixheaders'
    if 'smithy.api#httpPayload' in mtr: return 'payload'
    if 'smithy.api#httpResponseCode' in mtr: return 'statuscode'
    return 'body'
def struct_members(tid):
    s=resolve(tid)
    if not s or s['type'] not in ('structure','union'): return []
    return list(s.get('members',{}).items())

ops=sorted([k for k,v in shapes.items() if v.get('type')=='operation'], key=sn)
def parse_segs(uri):
    segs=[]
    for part in uri.split('?')[0].strip('/').split('/'):
        if part=='' : continue
        if part.startswith('{') and part.endswith('+}'):
            segs.append(('greedy',part[1:-2]))
        elif part.startswith('{') and part.endswith('}'):
            segs.append(('label',part[1:-1]))
        else:
            segs.append(('fixed',part))
    return segs

meta={}
for k in ops:
    v=shapes[k]; name=sn(k); tr=v.get('traits',{}); http=tr.get('smithy.api#http',{})
    method=http['method']; uri=http['uri']
    segs=parse_segs(uri)
    inp=v.get('input',{}).get('target'); out=v.get('output',{}).get('target')
    errs=[sn(e['target']) for e in v.get('errors',[])]
    # verb
    if name.startswith(('Create','Register')): verb='create'
    elif name.startswith(('Describe','Get')): verb='get'
    elif name.startswith('List'): verb='list'
    elif name.startswith('Update'): verb='update'
    elif name.startswith('Delete'): verb='delete'
    else: verb='action'
    label_segs=[s for s in segs if s[0] in ('label','greedy')]
    last_fixed = segs and segs[-1][0]=='fixed'
    # reclassify: crud verb but trailing fixed segment OR no label -> action/singleton
    if verb in ('create','get','update','delete'):
        if last_fixed or not label_segs:
            verb='action'
    if verb=='list' and False: pass
    rtype = segs[0][1] if segs and segs[0][0]=='fixed' else 'x'
    # validation rules top-level input members
    rules=[]
    if inp and sn(inp)!='Unit':
        for mn,m in struct_members(inp):
            mtr=m.get('traits',{})
            bind=binding(mtr)
            if bind in ('statuscode','queryparams','prefixheaders','payload'): continue
            req='smithy.api#required' in mtr
            tid=m['target']; kn=kind_of(tid)
            mn_min,mn_max,rmin,rmax=constraints(tid,mtr)
            evs=enum_values(tid) if kn=='Str' else None
            # only include if something to validate
            if not (req or mn_min is not None or mn_max is not None or rmin is not None or rmax is not None or evs):
                continue
            mtr2=m.get('traits',{})
            if bind=='query': wname=mtr2.get('smithy.api#httpQuery', mn)
            elif bind=='header': wname=mtr2.get('smithy.api#httpHeader', mn)
            elif bind=='label': wname=mn
            else: wname=wire_name(mn,m)
            rules.append({'name':mn,'wire':wname,'src':bind,'req':req,'kind':kn,
                'min_len':mn_min,'max_len':mn_max,'min_val':rmin,'max_val':rmax,'enums':evs})
    # output members
    omembers=[]
    if out and sn(out)!='Unit':
        for mn,m in struct_members(out):
            mtr=m.get('traits',{})
            if binding(mtr) not in ('body','payload'): continue
            omembers.append({'wire':wire_name(mn,m),'kind':kind_of(m['target'])})
    # list element (first list-typed output member). `list_scalar` is set when
    # the element is a plain scalar (e.g. `list<string>` name lists such as
    # ListCustomMetrics.metricNames): the engine then serialises each element as
    # the stored resource's identifier string rather than as an object.
    list_elem=None
    list_scalar=False
    if out and sn(out)!='Unit':
        for mn,m in struct_members(out):
            if binding(m.get('traits',{})) not in ('body','payload'): continue
            if kind_of(m['target'])=='List':
                ls=resolve(m['target'])
                elt=ls.get('member',{}).get('target') if ls else None
                ek=kind_of(elt) if elt else 'Struct'
                if ek in ('Str','Int','Num','Bool','Blob','Ts'):
                    list_scalar=True
                elems=[]
                if elt:
                    for emn,em in struct_members(elt):
                        if binding(em.get('traits',{})) not in ('body','payload'): continue
                        elems.append({'wire':wire_name(emn,em),'kind':kind_of(em['target'])})
                list_elem={'wire':wire_name(mn,m),'elems':elems}
                break
    # A required @httpPayload member: the whole request body is that member, so
    # the generic validator enforces its presence (an empty body is a client
    # error) even though payload members are not part of the per-member rules.
    req_payload=False
    if inp and sn(inp)!='Unit':
        for mn,m in struct_members(inp):
            mtr=m.get('traits',{})
            if 'smithy.api#httpPayload' in mtr and 'smithy.api#required' in mtr:
                req_payload=True
                break
    meta[name]={'method':method,'segs':segs,'verb':verb,'rtype':rtype,'errors':errs,
        'rules':rules,'omembers':omembers,'list_elem':list_elem,'list_scalar':list_scalar,
        'req_payload':req_payload,
        'nlabels':len(label_segs),'has_input': bool(inp and sn(inp)!='Unit')}
json.dump(meta,open('iot_meta.json','w'))
print("ops:",len(meta))
# stats
from collections import Counter
print(Counter(m['verb'] for m in meta.values()))

# ---- Rust emitter ----
def rs(s):
    return '"'+s.replace('\\','\\\\').replace('"','\\"')+'"'
def opt_i(x):
    return "None" if x is None else f"Some({int(x)})"
def kind(k): return f"K::{k}"
out=[]
out.append("// @generated by scripts/generate-iot-tables.py (do not edit by hand)")
out.append("// AWS IoT Core (iot) operation metadata derived from the Smithy model.")
out.append("#![allow(clippy::all)]")
out.append("")
out.append("#[derive(Clone, Copy, PartialEq, Eq, Debug)]")
out.append("pub enum Seg { Fixed(&'static str), Label(&'static str), Greedy(&'static str) }")
out.append("")
out.append("#[derive(Clone, Copy, PartialEq, Eq, Debug)]")
out.append("pub enum K { Str, Int, Num, Bool, Blob, Ts, List, Map, Struct }")
out.append("")
out.append("#[derive(Clone, Copy, PartialEq, Eq, Debug)]")
out.append("pub enum Src { Label, Query, Header, Body }")
out.append("")
out.append("#[derive(Clone, Copy, PartialEq, Eq, Debug)]")
out.append("pub enum Verb { Create, Get, List, Update, Delete, Action }")
out.append("")
out.append("pub struct Rule {")
out.append("    pub wire: &'static str,")
out.append("    pub src: Src,")
out.append("    pub req: bool,")
out.append("    pub kind: K,")
out.append("    pub min_len: Option<u64>,")
out.append("    pub max_len: Option<u64>,")
out.append("    pub min_val: Option<i64>,")
out.append("    pub max_val: Option<i64>,")
out.append("    pub enums: &'static [&'static str],")
out.append("}")
out.append("")
out.append("pub struct OpMeta {")
out.append("    pub op: &'static str,")
out.append("    pub method: &'static str,")
out.append("    pub segs: &'static [Seg],")
out.append("    pub verb: Verb,")
out.append("    pub rtype: &'static str,")
out.append("    pub nlabels: usize,")
out.append("    pub has_input: bool,")
out.append("    pub errors: &'static [&'static str],")
out.append("    pub rules: &'static [Rule],")
out.append("    pub omembers: &'static [(&'static str, K)],")
out.append("    pub list_field: Option<&'static str>,")
out.append("    pub list_elems: &'static [(&'static str, K)],")
out.append("    pub list_scalar: bool,")
out.append("    pub req_payload: bool,")
out.append("}")
out.append("")
verbmap={'create':'Create','get':'Get','list':'List','update':'Update','delete':'Delete','action':'Action'}
srcmap={'label':'Label','query':'Query','header':'Header','body':'Body','payload':'Body'}
out.append("pub static OPS: &[OpMeta] = &[")
for name in sorted(meta):
    m=meta[name]
    segs=[]
    for t,val in m['segs']:
        if t=='fixed': segs.append(f"Seg::Fixed({rs(val)})")
        elif t=='label': segs.append(f"Seg::Label({rs(val)})")
        else: segs.append(f"Seg::Greedy({rs(val)})")
    segs_s="&[" + ", ".join(segs) + "]"
    errs="&[" + ", ".join(rs(e) for e in m['errors']) + "]"
    # rules
    rules=[]
    for r in m['rules']:
        enums="&[" + ", ".join(rs(e) for e in (r['enums'] or [])) + "]"
        rules.append(
            "Rule { wire: %s, src: Src::%s, req: %s, kind: %s, min_len: %s, max_len: %s, min_val: %s, max_val: %s, enums: %s }" % (
            rs(r['wire']), srcmap[r['src']], 'true' if r['req'] else 'false', kind(r['kind']),
            opt_i(r['min_len']), opt_i(r['max_len']), opt_i(r['min_val']), opt_i(r['max_val']), enums))
    rules_s="&[" + ", ".join(rules) + "]"
    omem="&[" + ", ".join(f"({rs(o['wire'])}, {kind(o['kind'])})" for o in m['omembers']) + "]"
    if m['list_elem']:
        lf=rs(m['list_elem']['wire'])
        le="&[" + ", ".join(f"({rs(e['wire'])}, {kind(e['kind'])})" for e in m['list_elem']['elems']) + "]"
        lf_s=f"Some({lf})"
    else:
        lf_s="None"; le="&[]"
    out.append("    OpMeta { op: %s, method: %s, segs: %s, verb: Verb::%s, rtype: %s, nlabels: %d, has_input: %s, errors: %s, rules: %s, omembers: %s, list_field: %s, list_elems: %s, list_scalar: %s, req_payload: %s }," % (
        rs(name), rs(m['method']), segs_s, verbmap[m['verb']], rs(m['rtype']), m['nlabels'],
        'true' if m['has_input'] else 'false', errs, rules_s, omem, lf_s, le,
        'true' if m['list_scalar'] else 'false', 'true' if m['req_payload'] else 'false'))
out.append("];")
out.append("")
out.append("pub static ACTIONS: &[&str] = &[")
for name in sorted(meta):
    out.append("    %s," % rs(name))
out.append("];")
out.append("")
open(os.path.join(REPO,"crates","fakecloud-iot","src","generated.rs"),"w").write("\n".join(out)+"\n")
print("wrote crates/fakecloud-iot/src/generated.rs")
