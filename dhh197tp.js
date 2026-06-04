// vlsimple
import {
	connect
} from 'cloudflare:sockets';
export default {
	async fetch(request, env, ctx) {
		try {
			const url = new URL(request.url);
			const upgradeHeader = request.headers.get("Upgrade");
			const proxyMatch = url.pathname.match(/^\/(.+[:=-]\d+)$/);
			if (upgradeHeader === "websocket") {
				globalThis.pxip = proxyMatch[1] || "proxyip.fxxk.dedyn.io=443"
				const [client, ws] = Object.values(new WebSocketPair());
				ws.accept();
				let remote = null,
					udpWriter = null,
					isDNS = false;
				new ReadableStream({
					start(ctrl) {
						ws.addEventListener('message', e => ctrl.enqueue(e.data));
						ws.addEventListener('close', () => {
							remote?.close();
							ctrl.close();
						});
						ws.addEventListener('error', () => {
							remote?.close();
							ctrl.error();
						});
						const early = request.headers.get('sec-websocket-protocol');
						if (early) {
							try {
								ctrl.enqueue(Uint8Array.from(atob(early.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0)).buffer);
							} catch {}
						}
					}
				}).pipeTo(new WritableStream({
					async write(data) {
						if (isDNS) return udpWriter?.write(data);
						if (remote) {
							const w = remote.writable.getWriter();
							await w.write(data);
							w.releaseLock();
							return;
						}
						if (data.byteLength < 24) return;
						const view = new DataView(data);
						const optLen = view.getUint8(17);
						const cmd = view.getUint8(18 + optLen);
						if (cmd !== 1 && cmd !== 2) return;
						let pos = 19 + optLen;
						const port = view.getUint16(pos);
						const type = view.getUint8(pos + 2);
						pos += 3;
						let addr = '';
						if (type === 1) {
							addr = `${view.getUint8(pos)}.${view.getUint8(pos + 1)}.${view.getUint8(pos + 2)}.${view.getUint8(pos + 3)}`;
							pos += 4;
						} else if (type === 2) {
							const len = view.getUint8(pos++);
							addr = new TextDecoder().decode(data.slice(pos, pos + len));
							pos += len;
						} else if (type === 3) {
							const ipv6 = [];
							for (let i = 0; i < 8; i++, pos += 2) ipv6.push(view.getUint16(pos).toString(16));
							addr = ipv6.join(':');
						} else return;
						const header = new Uint8Array([data[0], 0]);
						const payload = data.slice(pos);
						if (cmd === 2) {
							if (port !== 53) return;
							isDNS = true;
							let sent = false;
							const {
								readable,
								writable
							} = new TransformStream({
								transform(chunk, ctrl) {
									for (let i = 0; i < chunk.byteLength;) {
										const len = new DataView(chunk.slice(i, i + 2)).getUint16(0);
										ctrl.enqueue(chunk.slice(i + 2, i + 2 + len));
										i += 2 + len;
									}
								}
							});
							readable.pipeTo(new WritableStream({
								async write(query) {
									try {
										const resp = await fetch('https://1.1.1.1/dns-query', {
											method: 'POST',
											headers: {
												'content-type': 'application/dns-message'
											},
											body: query
										});
										if (ws.readyState === 1) {
											const result = new Uint8Array(await resp.arrayBuffer());
											ws.send(new Uint8Array([...(sent ? [] : header), result.length >> 8, result.length & 0xff, ...result]));
											sent = true;
										}
									} catch {}
								}
							}));
							udpWriter = writable.getWriter();
							return udpWriter.write(payload);
						}
						let sock = null;
						try {
							sock = connect({
								hostname: addr,
								port: port
							});
							await sock.opened;
						} catch {
							sock = connect({
								hostname: globalThis.pxip.split(/[:=-]/)[0],
								port: globalThis.pxip.split(/[:=-]/)[1]
							});
						}
						await sock.opened;
						if (!sock) return;
						remote = sock;
						const w = sock.writable.getWriter();
						await w.write(payload);
						w.releaseLock();
						let sent = false;
						sock.readable.pipeTo(new WritableStream({
							write(chunk) {
								if (ws.readyState === 1) {
									ws.send(sent ? chunk : new Uint8Array([...header, ...new Uint8Array(chunk)]));
									sent = true;
								}
							},
							close: () => ws.readyState === 1 && ws.close(),
							abort: () => ws.readyState === 1 && ws.close()
						})).catch(() => {});
					}
				})).catch(() => {});
				return new Response(null, {
					status: 101,
					webSocket: client
				});
			}
			return new Response(`hello world`);
		} catch (err) {
			return new Response(`An error occurred: ${err.toString()}`, {
				status: 500,
			});
		}
	},
};
