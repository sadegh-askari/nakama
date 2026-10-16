import pathlib, re, json, hashlib, shutil, collections, sys
client=pathlib.Path(sys.argv[1]); root=pathlib.Path(sys.argv[2]); out=root/'currency-art'
def read(p): return p.read_text(encoding='utf-8-sig')
def section(text,key):
 m=re.search(r'^  '+re.escape(key)+r':\n(.*?)(?=^  \w|\Z)',text,re.M|re.S)
 return m.group(1) if m else ''
def dictionary(text):
 return {m[0]:m[1] for m in re.findall(r'^    - Key: (.*?)\n(.*?)(?=^    - Key:|\Z)',text,re.M|re.S)}
def decode(text):
 key=re.search(r'cryptoKey: ([0-9a-f]+)',text); val=re.search(r'hiddenChars: ([0-9a-f]+)',text)
 if not key or not val: return ''
 k=bytes.fromhex(key[1]).decode('utf-16-le'); v=bytes.fromhex(val[1]).decode('utf-16-le')
 return ''.join(chr(ord(c)^ord(k[i%len(k)])) for i,c in enumerate(v))
def refs(text):
 return {rid:name for rid,name in re.findall(r'- rid: (-?\d+)\n      type: \{class: LocalImageAsset[^\n]*\n      data:\n        Id: ([^\n]+)',text)}
guids={}
for p in (client/'Assets').rglob('*.meta'):
 m=re.search(r'^guid: (\w+)',read(p),re.M)
 if m: guids[m[1]]=p.with_suffix('')
assets=read(client/'Assets/SO/Singleton/AssetCollection.asset')
local=dictionary(section(assets,'LocalImageAssets')); characters=dictionary(section(assets,'CharacterAssets'))
sources={}; missing=[]; counts=collections.Counter()
def add(id,ref,collection):
 m=re.search(r'm_AssetGUID: ([0-9a-f]{32})',ref or '')
 source=guids.get(m[1]) if m else None
 if not source or source.suffix.lower() not in ('.png','.jpg','.jpeg'): missing.append({'id':id,'collection':collection}); return
 name=hashlib.sha256(source.read_bytes()).hexdigest()[:16]+source.suffix.lower()
 out.mkdir(exist_ok=True); shutil.copyfile(source,out/name)
 sources[id]={'source':str(source.relative_to(client)), 'file':name,'collection':collection}; counts[collection]+=1
curr=read(client/'Assets/SO/Singleton/CurrencyCollection.asset')
for id,body in re.findall(r'^  - Id: ([^\n]+)\n(.*?)(?=^  - Id:|\Z)',section(curr,'_currencies'),re.M|re.S): add(id,body,'CurrencyCollection')
char=read(client/'Assets/SO/Singleton/CharacterCollection.asset')
for guid in re.findall(r'guid: (\w+)',section(char,'_characters')):
 p=guids[guid]; data=read(p); m=re.search(r'^  <Id>k__BackingField:\n(.*?)(?=^  \S)',data,re.M|re.S); id=decode(m[1])
 body=characters.get(id,''); portrait=re.search(r'        CharacterHeadArt:\n(.*?)(?=^        \w)',body,re.M|re.S)
 if not portrait or not re.search(r'm_AssetGUID: [0-9a-f]{32}',portrait[1]): portrait=re.search(r'        BattleHeadArt:\n(.*?)(?=^        \w)',body,re.M|re.S)
 add(id,portrait[1] if portrait else '', 'CharacterCollection')
for collection,key,art in [('GearCollection','Items','Icon'),('AvatarAssetCollection','Frames','Artwork')]:
 text=read(client/f'Assets/SO/StorageObject/Definition/{collection}.asset'); references=refs(text)
 for entry in re.split(r'^  - Id:\n',section(text,key),flags=re.M)[1:]:
  id=decode(entry.split(f'    {art}:')[0]); match=re.search(r'    '+art+r':\n      rid: (-?\d+)',entry)
  image=references.get(match[1],'') if match else ''
  add(id,local.get(image,''),collection)
for id, entry in list(sources.items()):
 if entry['collection']=='CharacterCollection':
  sources.setdefault(id+'_Shard',dict(entry))
add('1_frame',local.get('1_frame',''),'AssetCollection')
add('PachinkoToken','m_AssetGUID: '+re.search(r'm_Sprite: \{fileID: \d+, guid: (\w+)',read(client/'Assets/Prefabs/Singleton/Reward/PachinkoTokenReward.prefab'))[1], 'PachinkoTokenReward')
add('SmokeBomb_lvl4_Elixir','m_AssetGUID: '+re.search(r'^guid: (\w+)', read(client/'Assets/Sprites/UI/Elixir/New Elixir Images/smok/SmokeLvl4.png.meta'),re.M)[1], 'ElixirCollection')
missing=[entry for entry in missing if entry['id'] not in sources]
bank=read(client/'Assets/SO/StorageObject/Definition/BankDef.asset')
bankids=list(dictionary(section(bank,'Currencies')))+list(dictionary(section(bank,'Items')))
obsolete=['LukeCageBracers_Gear','LukeCageBracers2_Gear','IonizedMembrane_Gear','Calsified_Membrane_Gear','BoneAugmentation_Gear','GripStrenghFormula_Gear','LegStrenghFormula_Gear','SynapticStimulant_Gear','ArsamKhooki_Shard','1_frame']
for id in bankids:
 if id=='Level' or id=='ViewedProfileLevel' or id.endswith('_Level'):
  add(id,'m_AssetGUID: '+re.search(r'^guid: (\w+)',read(client/'Assets/Sprites/UI/Battle_Old/battle page-player character level-icon.png.meta'),re.M)[1], 'Progression UI')
missing=[entry for entry in missing if entry['id'] not in obsolete]
report={'obsoleteBankItems':[id for id in obsolete if id in bankids],'mapped':len(sources),'byCollection':dict(counts),'missingCollectionArt':missing,'bankItemsWithoutArt':[id for id in bankids if id not in sources and id not in obsolete]}
(out/'sources.json').write_text(json.dumps(sources,indent=2,sort_keys=True)+'\n')
(out/'coverage.json').write_text(json.dumps(report,indent=2)+'\n')
p=root/'currency-art.mjs'; module=p.read_text(); start=module.index('Object.freeze(')+len('Object.freeze('); end=module.index(');',start)
mapping={id:'/currency-art/'+row['file'] for id,row in sources.items()}; p.write_text(module[:start]+json.dumps(mapping,indent=2,sort_keys=True)+module[end:])
excluded=root/'obsolete-items.mjs'; excluded.write_text('export const ObsoleteItemIds = new Set('+json.dumps(obsolete,indent=2)+');\n')
print(json.dumps(report,indent=2))
