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
  elements:{name:{}, password:{}, email:{}, code:{}}, reset(){this.value=''},
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
 run(source); respond('auth/config',{shared:false}); await tick(); if (boot) { respond('me',{},401); await tick(); respond('limits',policy); await tick(); }
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

test('shared topic data cannot repopulate another account',async()=>{
 const a=await app();a.run('sharedAuth=true');
 const topics=a.run('refreshSharedTopics()').catch(()=>{});await tick();
 a.run('resetAccount({id:"B",name:"Bob",interests:[],onboarded:true})');
 a.respond('shared',{topics:[{topic:'Private topic',requested:true}]});await topics;
 assert.equal(a.get('shared-list').children.length,0);
});
test('shared topic text is rendered as text and carries an explicit action',async()=>{
 const a=await app();a.run('sharedAuth=true');
 const topics=a.run('refreshSharedTopics()');await tick();
 const pending=a.pending.find(p=>p.url==='/api/shared');
 assert.deepEqual(JSON.parse(pending.opts.body),{tool:'cosift_topics',action:'list'});
 a.respond('shared',{topics:[{topic:'<img src=x onerror=alert(1)>',requested:true}]});await topics;
 assert.equal(a.get('shared-list').children[0].children[0].textContent,'<img src=x onerror=alert(1)>');
});
test('shared view is unavailable to a guest',async()=>{
 const a=await app();a.run('resetAccount(); sharedAuth=true');
 a.get('view-shared').hidden=true;await a.run('view("shared")');
 assert.equal(a.get('view-shared').hidden,true);
});
test('email-code login waits for verification and prevents switching during an active request',async()=>{
 const a=await app();a.run('resetAccount();sharedAuth=true');
 const form=a.get('auth-form');form.elements.email.value='shared@example.com';form.elements.code.value='123456';
 a.context.FormData=class {constructor(form){this.form=form}get(key){return this.form.elements[key]?.value || ''}};
 const start=form.onsubmit({preventDefault(){},target:form});await tick();
 assert.equal(a.run('authBusy'),true);
 a.respond('auth/start',{request_id:'test-challenge',expires_at:'2030-01-01T00:00:00Z'});await start;
 assert.equal(a.run('user'),null);
 assert.equal(a.get('code-field').hidden,false);
 assert.equal(form.elements.email.readOnly,true);
 assert.equal(a.run('authBusy'),false);
 const verify=form.onsubmit({preventDefault(){},target:form});await tick();
 a.get('auth-restart').onclick();
 assert.equal(a.run('authChallenge.request_id'),'test-challenge');
 assert.deepEqual(JSON.parse(a.pending.find(p=>p.url==='/api/auth/verify').opts.body),{request_id:'test-challenge',code:'123456'});
 a.respond('auth/verify',{error:'Wrong or expired code'},401);await verify;
 assert.equal(a.run('user'),null);
 a.get('auth-restart').onclick();
 assert.equal(a.run('authChallenge'),null);
 assert.equal(form.elements.email.readOnly,false);
 assert.equal(a.get('code-field').hidden,true);
});
test('guests are sent to login before viewing or submitting contributions', async () => {
 const a = await app(); a.run('user=null');
 await a.run('view("contribute")');
 assert.equal(a.get('auth').hidden,false);
 a.get('contribution-form').onsubmit({preventDefault(){},target:a.get('contribution-form')});
 assert.equal(a.pending.some(p=>p.url==='/api/submissions'),false);
 assert.match(a.get('notice').textContent,/Sign in/);
});
test('monthly credits render actual totals and clear on account reset', async () => {
 const a=await app(); const refresh=a.run('refreshCredits()'); await tick();
 a.respond('credits',{balance:42,monthly:{month:'2026-09',earned:10,purchased:50,spent:18}}); await refresh;
 assert.equal(a.get('monthly-credits').hidden,false);
 assert.equal(a.get('month-earned').textContent,'10');
 assert.equal(a.get('month-spent').textContent,'18');
 assert.match(a.get('credit-month').textContent,/2026-09/);
 a.run('resetAccount()'); assert.equal(a.get('monthly-credits').hidden,true);
});
test('analytics emits a pageview without query strings or account fields', async () => {
 const a=await app(); a.context.window={}; a.context.document.head=node();
 Object.assign(a.context.location,{origin:'https://cosift.example',pathname:'/login',search:'?email=private@example.com&token=secret'});
 const loading=a.run('loadAnalytics()'); await tick();
 a.respond('analytics',{measurement_id:'G-XVRJ3595D1'}); await loading;
 const events=JSON.stringify(a.context.window.dataLayer);
 assert.match(events,/page_view/); assert.match(events,/https:\/\/cosift.example\/login/);
 assert.doesNotMatch(events,/private@example|secret|Alice|user_id/);
 assert.equal(a.context.document.head.children[0].src,'https://www.googletagmanager.com/gtag/js?id=G-XVRJ3595D1');
});
test('temporary session failure keeps login hidden and offers a working startup retry', async () => {
 const a=await app(false); a.run('user=null');
 assert.equal(a.get('boot').hidden,false);
 assert.equal(a.get('auth').hidden,true);
 a.respond('me',{error:'Account provider unavailable'},503); await tick();
 assert.equal(a.get('boot').hidden,false);
 assert.equal(a.get('auth').hidden,true);
 assert.equal(a.get('boot-retry').hidden,false);
 assert.match(a.get('boot-message').textContent,/try again/i);
 const retry=a.get('boot-retry').onclick(); await tick();
 a.respond('auth/config',{shared:true}); await tick();
 a.respond('me',{},401); await tick();
 a.respond('limits',policy); await retry;
 assert.equal(a.get('boot').hidden,true);
 assert.equal(a.get('auth').hidden,false);
});
test('a failed contribution keeps its draft and reenables submission', async () => {
 const a=await app(); const form=a.get('contribution-form');
 a.get('urls').value='https://example.org/useful-article';
 form.onsubmit({preventDefault(){},target:form}); await tick();
 a.respond('submissions',{error:'Daily contribution limit reached. Retry in an hour.'},429); await tick();
 assert.equal(a.get('urls').value,'https://example.org/useful-article');
 assert.equal(form.querySelector().disabled,false);
 assert.match(a.get('notice').textContent,/Daily contribution limit/);
});
test('billing shows weighted costs and live availability without enabling unconfigured checkout', async () => {
 const a=await app(); const refresh=a.run('refreshCredits()'); await tick();
 a.respond('credits',{balance:1000,monthly_free_credits:1000,payments_enabled:false,payment_mode:'unavailable',credit_pack:{amount_cents:500,currency:'usd',credits:50000}}); await refresh;
 assert.equal(a.get('billing-balance').textContent,'1,000');
 assert.equal(a.get('billing-pack-price').textContent,'$5.00');
 assert.equal(a.get('buy-credits').hidden,true);
 assert.match(a.get('payment-info').textContent,/Search 1 credit.*Answer 2 credits.*Research 3 credits/);
});
test('expired checkout can be retried with a new idempotency key', async () => {
 const a=await app(); const checkout=a.get('buy-credits').onclick(); await tick();
 a.respond('payments/checkout',{error:'Checkout expired'},409); await checkout;
 assert.equal(a.run('checkoutKey'),undefined);
 assert.equal(a.get('buy-credits').disabled,false);
});
