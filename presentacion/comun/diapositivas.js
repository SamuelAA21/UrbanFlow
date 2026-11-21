/* Kit HTML — motor de diapositivas.
   - Cada diapositiva es <section class="diapo" data-escena="nombre-opcional">.
   - Los elementos con data-paso="n" aparecen en el paso n (1, 2, 3…).
   - Una escena registrada con Diapositivas.escena(nombre, { pasos, entrar, paso, salir }) controla animaciones propias.
   - [data-contar="19"] cuenta de 0 a 19 cuando se vuelve visible.
   - Teclas: → Espacio PageDown (siguiente), ← PageUp (anterior), Inicio, Fin, F (pantalla completa), N (notas), O (vista general).
   - URL: presentacion.html#5 abre la diapositiva 5; #5.99 la abre con todos sus pasos visibles. */
(function () {
  'use strict';
  const escenas = {};
  const reducido = () => window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  window.Diapositivas = { escena: (nombre, def) => { escenas[nombre] = def; } };

  function contar(el) {
    const fin = Number(el.dataset.contar);
    if (reducido()) { el.textContent = fin; return; }
    const t0 = performance.now();
    const dur = 900;
    const cuadro = (t) => {
      const k = Math.min(1, (t - t0) / dur);
      el.textContent = Math.round(fin * (1 - Math.pow(1 - k, 3)));
      if (k < 1) requestAnimationFrame(cuadro);
    };
    requestAnimationFrame(cuadro);
  }

  function iniciar() {
    const escenario = document.querySelector('.escenario');
    const diapos = [...escenario.querySelectorAll('.diapo')];
    const barra = Object.assign(document.createElement('div'), { className: 'progreso' });
    const cont = Object.assign(document.createElement('div'), { className: 'contador' });
    const notas = Object.assign(document.createElement('div'), { className: 'panel-notas' });
    const general = Object.assign(document.createElement('div'), { className: 'vista-general' });
    const ayuda = Object.assign(document.createElement('div'), { className: 'ayuda', textContent: '→ avanzar, ← volver, P pausar, R repetir, F pantalla completa, N notas, O vista general' });
    const estadoMov = Object.assign(document.createElement('div'), { className: 'estado-movimiento', textContent: 'En pausa: P para continuar' });
    document.body.append(barra, cont, notas, general, ayuda, estadoMov);
    setTimeout(() => { ayuda.style.opacity = 0; }, 5000);

    /* v2: dirección artística del proyecto (guion.js) */
    const direccion = (window.REPO_GUION || {}).direccion || {};
    if (direccion.registro) document.body.dataset.registro = direccion.registro;
    if (direccion.densidad) document.body.dataset.densidad = direccion.densidad;
    if (direccion.acento) document.documentElement.style.setProperty('--acento', direccion.acento);

    const ajustar = () => {
      const s = Math.min(window.innerWidth / 1920, window.innerHeight / 1080);
      escenario.style.transform = `translate(-50%, -50%) scale(${s})`;
    };
    window.addEventListener('resize', ajustar);
    ajustar();

    diapos.forEach((d, i) => {
      d.dataset.n = i + 1;
      const pie = d.querySelector('.pie-diapo .n');
      if (pie) pie.textContent = `${i + 1} / ${diapos.length}`;
      const fr = [...d.querySelectorAll('[data-paso]')];
      const esc = escenas[d.dataset.escena];
      const pasosEsc = esc ? (typeof esc.pasos === 'function' ? esc.pasos(d) : (esc.pasos || 0)) : 0;
      d._total = Math.max(0, ...fr.map((e) => Number(e.dataset.paso) || 1), pasosEsc);
    });

    let i = -1;
    let p = 0;
    const titulo = (d) => { const h = d.querySelector('h1, h2'); return h ? h.textContent.trim() : `Diapositiva ${d.dataset.n}`; };

    function pintarPasos(d, paso) {
      d.querySelectorAll('[data-paso]').forEach((e) => {
        const n = Number(e.dataset.paso) || 1;
        const antes = e.classList.contains('visto');
        e.classList.toggle('visto', n <= paso);
        e.classList.toggle('superado', n < paso);
        if (!antes && n <= paso) e.querySelectorAll('[data-contar]').forEach(contar);
        if (!antes && n <= paso && e.matches('[data-contar]')) contar(e);
      });
    }

    function ir(ni, np, dir) {
      ni = Math.max(0, Math.min(diapos.length - 1, ni));
      const nueva = diapos[ni];
      np = Math.max(0, Math.min(nueva._total, np));
      if (ni !== i) {
        const vieja = diapos[i];
        if (vieja) {
          const ev = escenas[vieja.dataset.escena];
          if (ev && ev.salir) ev.salir(vieja);
          vieja.classList.remove('actual');
          vieja.classList.toggle('pasada', ni > i);
        }
        nueva.classList.remove('pasada');
        nueva.classList.add('actual');
        nueva.querySelectorAll(':scope > :not([data-paso]) [data-contar], :scope > [data-contar]:not([data-paso])').forEach((e) => { if (!e.closest('[data-paso]')) contar(e); });
        const en = escenas[nueva.dataset.escena];
        if (en && en.entrar) en.entrar(nueva, np);
        i = ni;
      }
      p = np;
      pintarPasos(nueva, p);
      const es = escenas[nueva.dataset.escena];
      if (es && es.paso) es.paso(nueva, p, dir || 1);
      barra.style.width = `${((i + (nueva._total ? p / (nueva._total + 1) : 0)) / (diapos.length - 1 || 1)) * 100}%`;
      cont.textContent = `${i + 1}/${diapos.length}${nueva._total ? `  paso ${p}/${nueva._total}` : ''}`;
      const nota = nueva.querySelector('.notas');
      const notaPaso = es && es.nota ? es.nota(nueva, p) : '';
      const sig = diapos[i + 1];
      notas.innerHTML = `<div>${nota ? nota.innerHTML : 'Sin notas para esta diapositiva.'}</div>${notaPaso ? `<div class="nota-paso" style="margin-top:10px;color:var(--acento)">Paso ${p}: ${notaPaso}</div>` : ''}<div class="siguiente">Diapositiva ${i + 1} de ${diapos.length}${nueva._total ? `, paso ${p} de ${nueva._total}` : ''}${sig ? `. Siguiente: ${titulo(sig)}` : ''}</div>`;
      estadoMov.classList.remove('on');
      history.replaceState(null, '', `#${i + 1}${p ? '.' + p : ''}`);
    }
    /* v2: control de la escena actual (pausar, reanudar, reiniciar, progreso), si la escena lo ofrece */
    const control = () => { const d = diapos[i]; const es = d && escenas[d.dataset.escena]; return es && es.control ? es.control(d) : null; };
    const alternarPausa = () => {
      const c = control();
      if (!c) return;
      c.alternar();
      estadoMov.classList.toggle('on', c.enPausa());
    };
    const repetir = () => { const c = control(); if (c) { c.reiniciar(); estadoMov.classList.remove('on'); } };
    const siguiente = () => { const d = diapos[i]; if (p < d._total) ir(i, p + 1, 1); else if (i < diapos.length - 1) ir(i + 1, 0, 1); };
    const anterior = () => { if (p > 0) ir(i, p - 1, -1); else if (i > 0) ir(i - 1, diapos[i - 1]._total, -1); };

    general.innerHTML = `<ol>${diapos.map((d, k) => `<li><button data-ir="${k}"><span>${k + 1}</span>${titulo(d)}</button></li>`).join('')}</ol>`;
    general.addEventListener('click', (e) => { const b = e.target.closest('[data-ir]'); if (b) { ir(Number(b.dataset.ir), 0); general.classList.remove('on'); } });

    document.addEventListener('keydown', (e) => {
      if (e.altKey || e.ctrlKey || e.metaKey) return;
      const k = e.key;
      if (['ArrowRight', ' ', 'PageDown', 'Enter'].includes(k)) { e.preventDefault(); siguiente(); }
      else if (['ArrowLeft', 'PageUp', 'Backspace'].includes(k)) { e.preventDefault(); anterior(); }
      else if (k === 'Home') ir(0, 0);
      else if (k === 'End') ir(diapos.length - 1, 0);
      else if (k === 'f' || k === 'F') { if (document.fullscreenElement) document.exitFullscreen(); else document.documentElement.requestFullscreen().catch(() => {}); }
      else if (k === 'n' || k === 'N') notas.classList.toggle('on');
      else if (k === 'p' || k === 'P') alternarPausa();
      else if (k === 'r' || k === 'R') repetir();
      else if (k === 'o' || k === 'O' || k === 'Escape') {
        if (k === 'Escape' && !general.classList.contains('on')) return;
        general.classList.toggle('on');
        general.querySelectorAll('button').forEach((b) => b.classList.toggle('actual', Number(b.dataset.ir) === i));
      }
    });
    let x0 = null;
    document.addEventListener('touchstart', (e) => { x0 = e.touches[0].clientX; }, { passive: true });
    document.addEventListener('touchend', (e) => {
      if (x0 == null) return;
      const dx = e.changedTouches[0].clientX - x0;
      if (Math.abs(dx) > 50) { if (dx < 0) siguiente(); else anterior(); }
      x0 = null;
    });

    /* API pública: navegación y control, usada por las pruebas automáticas (probar_entregables.py) */
    window.Presentacion = {
      ir: (n, paso) => ir(n - 1, paso || 0),
      siguiente,
      anterior,
      estado: () => ({ diapo: i + 1, paso: p, pasosDiapo: diapos[i]._total, diapos: diapos.length, escena: diapos[i].dataset.escena || null }),
      control
    };

    /* URL: #6.2 (diapositiva 6, paso 2) y #6.2@0.5 (mismo paso, a mitad de su animación) */
    const m = location.hash.match(/^#(\d+)(?:\.(\d+))?(?:@([\d.]+))?/);
    ir(m ? Number(m[1]) - 1 : 0, m && m[2] ? Number(m[2]) : 0);
    if (m && m[3] != null) { const c = control(); if (c) c.progreso(p, Number(m[3])); }
  }
  window.addEventListener('load', iniciar);
})();
