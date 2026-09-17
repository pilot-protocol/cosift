// Tests run the shipped browser script with controlled asynchronous HTTP responses.
// The small DOM double tests account/request logic; browser checks cover layout and interaction.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../web/app.js'), 'utf8');
const policy = {guest_interval_seconds:60, guest:{search:{requests:1, window_seconds:60}},member:{search:{requests:60,window_seconds:60}},member_free_requests_per_minute:60};
const tick = () => new Promise(setImmediate);
function node() {
 return {hidden:false, disabled:false, value:'', textContent:'', children:[], dataset:{}, files:[],
  classList:{toggle(){}}, setAttribute(){}, removeAttribute(){},
  elements:{name:{}, password:{}}, reset(){this.value=''},
  append(...items){this.children.push(...items)}, replaceChildren(...items){this.children=items},
  querySelector(){return this.button ||= node()}, click(){return this.onclick?.()},
 };
}
async function app(boot = true) {
 const nodes = new Map(), pending = [];
 const get = id => { if(!nodes.has(id)) nodes.set(id,node()); return nodes.get(id) };
 const context = vm.createContext({document:{getElementById:get, createElement:node, createTextNode:text=>({textContent:text}),querySelectorAll:()=>[]},
  URL, URLSearchParams, FormData, AbortController, DOMException, Intl, console,
  crypto:{randomUUID:()=> 'test-checkout-key'}, location:{pathname:'/',search:'',assign:url=>{context.destination=url}}, history:{replaceState(){}},
  setTimeout:()=>0,clearTimeout(){},setInterval(){},
  fetch:(url,opts)=>new Promise(resolve=>pending.push({url,opts,resolve})),
 });
 const run = code => vm.runInContext(code,context);
 const respond = (route,data,status=200) => {
  const i = pending.findIndex(p=>p.url==='/api/'+route);
  assert.notEqual(i,-1,'pending '+route);
  pending.splice(i,1)[0].resolve({ok:status<400,status,json:async()=>data});
 };
 run(source); if (boot) { respond('me',{},401); await tick(); respond('limits',policy); await tick(); }
 run('user = {id:"A",name:"Alice",interests:[],onboarded:true}');
 return {run,respond,get,pending,context};
}
test('logout during search clears private state and permits another search',async()=>{
 const a=await app();
 const search=a.run('runSearch("private query")'); await tick();
 const logout=a.get('logout').onclick(); await tick();
 a.respond('logout',{}); await logout;
 a.respond('search?q=private%20query',{hits:[{title:'Private result',url:'https://example.com'}]}); await search;
 assert.equal(a.get('search-form').querySelector().disabled,false);
 assert.equal(a.run('currentQuery'),'');
 assert.equal(a.get('results').children.length,0);
 assert.equal(a.get('auth').hidden,false);
});
test('late saved response cannot repopulate a logged-out account',async()=>{
 const a=await app(); const saved=a.run('refreshSaved()').catch(()=>{}); await tick();
 const logout=a.get('logout').onclick(); await tick(); a.respond('logout',{}); await logout;
 a.respond('saved',[{id:'secret',query:'Alice private query',mode:'search',created_at:1}]); await saved;
 assert.equal(a.run('saved.length'),0);
 assert.equal(a.get('saved-list').children.length,0);
});
test('expired session clears private results and contribution drafts',async()=>{
 const a=await app(); a.run('currentQuery="private"; saved=[{query:"private"}]; $("urls").value="https://example.com/private"; $("results").append(el("p","private"))');
 const request=a.run('api("credits")').catch(()=>{}); await tick(); a.respond('credits',{error:'Sign in'},401); await request;
 assert.equal(a.run('currentQuery'),''); assert.equal(a.run('saved.length'),0);
 assert.equal(a.get('results').children.length,0); assert.equal(a.get('urls').value,'');
});
test('logout prevents a delayed checkout from navigating',async()=>{
 const a=await app(); const checkout=a.get('buy-credits').onclick(); await tick();
 const logout=a.get('logout').onclick(); await tick(); a.respond('logout',{}); await logout;
 a.respond('payments/checkout',{url:'https://checkout.stripe.com/c/pay/test'}); await checkout;
 assert.equal(a.context.destination,undefined);
});
test('a stale unauthorized response cannot log out a newer account',async()=>{
 const a=await app(); const saved=a.run('refreshSaved()').catch(()=>{}); await tick();
 const logout=a.get('logout').onclick(); await tick(); a.respond('logout',{}); await logout;
 a.run('user={id:"B",name:"Bob",interests:[],onboarded:true}');
 a.respond('saved',{error:'Expired'},401); await saved;
 assert.equal(a.run('user?.id'),'B');
});
test('a newer search aborts the old request and keeps its own result',async()=>{
 const a=await app();const old=a.run('runSearch("old")');await tick();
 const oldRequest=a.pending.find(p=>p.url==='/api/search?q=old');
 const current=a.run('runSearch("current")');await tick();
 assert.equal(oldRequest.opts.signal.aborted,true);
 a.respond('search?q=current',{hits:[]});await tick();a.respond('credits',{balance:10});await current;
 a.respond('search?q=old',{hits:[{title:'Old result'}]});await old;
 assert.equal(a.run('currentQuery'),'current');
 assert.equal(a.get('search-form').querySelector().disabled,false);
});
test('late startup session check does not overwrite a new login',async()=>{
 const a=await app(false);
 a.run('resetAccount({id:"B",name:"Bob",interests:[],onboarded:true})');
 a.respond('me',{error:'Not signed in'},401);await tick();
 assert.equal(a.run('user?.id'),'B');
});
