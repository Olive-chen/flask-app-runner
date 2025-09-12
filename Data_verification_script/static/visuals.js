const PALETTE = ['#6366F1','#10B981','#F59E0B','#F43F5E','#06B6D4','#A78BFA','#22C55E','#F97316'];

function createGradient(ctx, color){
  const g = ctx.createLinearGradient(0, 0, 0, 220);
  g.addColorStop(0, color + 'CC');
  g.addColorStop(1, color + '10');
  return g;
}

let CHARTS = {};
function destroyCharts(){ Object.values(CHARTS).forEach(c=>c && typeof c.destroy==='function' && c.destroy()); CHARTS = {}; }

function renderCharts(summary){
  window.summary = summary;   // 全局可访问
  if(!summary){ console.warn('renderCharts: no summary'); return; }
  destroyCharts();

  // four_types —— 强制文本类目 + 不自动跳过刻度
  try {
    const distRaw = summary.four_types_distribution || (summary.four_types && summary.four_types.distribution) || [];
    if (distRaw.length) {
      const ctx = document.getElementById('chartFourTypes').getContext('2d');
      const labels = distRaw.map(d => {
        const v = (d && (d.value ?? d.four_types ?? d.label)) != null ?
      String(d.value ?? d.four_types ?? d.label) : '';
        return v;});
      const counts = distRaw.map(d => Number(d && (d.count ?? d.freq) || 0));
      const colors = ['#22C55E', '#ffd900', '#3B82F6', '#EF4444', '#6B7280'];

      CHARTS.ft = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [{ label: 'count', data: counts, backgroundColor: colors }] },
        options: {
          responsive: true,
          maintainAspectRatio: true,   // ← 新增
          plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false } },
          scales: {
            x: { type: 'category', title: { display: true, text: 'four_types 値（テキスト）' }, ticks: { autoSkip: false } },
            y: { beginAtZero: true, title: { display: true, text: '件数' } }
          },
          animation: { duration: 800, easing: 'easeOutQuart' }
        }
      });
    }
  } catch (e) { console.error('four_types chart error', e); }

  // gender
  try {
    let gdist = summary.dynamodb_json && summary.dynamodb_json.gender_distribution;
    if (Array.isArray(gdist)) {
      const tmp = {}; gdist.forEach(it=>{ if(it && it.gender){ tmp[String(it.gender)] = (tmp[String(it.gender)]||0) + Number(it.count||0); } }); gdist = tmp;
    }
    if (gdist && Object.keys(gdist).length) {
      const labels = Object.keys(gdist);
      const counts = labels.map(k => Number(gdist[k]));
      const ctx = document.getElementById('chartGender').getContext('2d');
      const colors = labels.map((_,i)=>PALETTE[(i+2)%PALETTE.length]);
      CHARTS.gender = new Chart(ctx, {
        type: 'doughnut',
        data: { labels, datasets: [{ data: counts, backgroundColor: colors, hoverOffset: 8 }] },
        options: { responsive: true, cutout: '70%', plugins: { legend: { position: 'bottom' } }, animation: { duration: 800, easing: 'easeOutQuart' } }
      });
    }
  } catch (e) { console.error('gender chart error', e); }

  // continuity
  try {
    const t = summary.time_continuity || {};
    const observed = Number((t.observed_points != null ? t.observed_points : t.rows) || 0);
    const missing = Number((t.missing_points_total_est != null ? t.missing_points_total_est : Math.max(0, Number(t.expected_points_est||0) - Number(t.observed_points||0))));
    const ctx = document.getElementById('chartContinuity').getContext('2d');
    CHARTS.cont = new Chart(ctx, {
      type: 'bar',
      data: { labels: ['観測', '推定欠損'], datasets: [{ data: [observed, missing], backgroundColor: [PALETTE[5], PALETTE[3]], borderWidth: 0, borderRadius: 10 }]},
      options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, title: { display: true, text: '件数' } } }, animation: { duration: 800, easing: 'easeOutQuart' } }
    });
  } catch (e) { console.error('continuity chart error', e); }

  // age —— 只显示“有计数”的年龄；刻度不跳过；单折线（后端未提供 Low/High）
  try {
    const ctx = document.getElementById('chartAge').getContext('2d');
    const ageCurve = summary.dynamodb_json && summary.dynamodb_json.age_distribution_curve;

    if (ageCurve && Array.isArray(ageCurve.data) && Array.isArray(ageCurve.labels)) {
      const rows = ageCurve.labels.map((lab,i)=>({lab, v:Number(ageCurve.data[i]||0)}));      const L = rows.map(r=>r.lab);
      const D = rows.map(r=>r.v);
      const grad = createGradient(ctx, PALETTE[2]);

      CHARTS.age = new Chart(ctx, {
        type: 'line',
        data: {
          labels: ageCurve.labels,
          datasets: [
            {
              label: 'count',                // ← 让第二行显示成 `count: 数值`
              data: ageCurve.data,
              fill: true,
              backgroundColor: grad,
              borderColor: PALETTE[2],
              borderWidth: 2,
              pointRadius: 0,
              tension: 0.3
            },
            // 如需 Low/High 端点，同理可保留，label 可改成别名
          ]
        },
        options: {
          responsive: true,
          plugins: {
            legend: { display: false },
            tooltip: {
              mode: 'index',
              intersect: false,
              callbacks: {
                title: (items) => String(items?.[0]?.label ?? ''),      // ↑ 顶部只显示“年龄”
                label: (ctx) => `count: ${ctx.formattedValue}`           // ↓ 第二行显示 count: 数值
              }
            }
          },
          scales: {
            x: { type: 'category', title: { display: true, text: '年齢（1歳刻み）' }, ticks: { autoSkip: false } },
            y: { beginAtZero: true, title: { display: true, text: '件数' } }
          },
          animation: { duration: 800, easing: 'easeOutQuart' }
        }
      });


    } else {
      const buckets = summary.dynamodb_json && summary.dynamodb_json.age_buckets;
      if (buckets && buckets.length) {
        const rows = buckets.map(b => ({lab:String(b.label||''), v:Number(b.count||0)})).filter(r=>r.v>0);
        const L = rows.map(r=>r.lab);
        const D = rows.map(r=>r.v);
        const grad = createGradient(ctx, PALETTE[2]);
        CHARTS.age = new Chart(ctx, {
          type: 'bar',
          data: { labels: L, datasets: [{ label: 'count', data: D, backgroundColor: grad, borderColor: PALETTE[2], borderWidth: 1.5, borderRadius: 8 }]},
          options: { 
            responsive: true, 
            plugins: { legend: { display: false } }, 
            scales: { 
              x: { type: 'category', title: { display: true, text: '年齢（区間）' }, ticks: { autoSkip: false } }, 
              y: { beginAtZero: true, title: { display: true, text: '件数' } } 
            }, 
            animation: { duration: 800, easing: 'easeOutQuart' } 
          }
        });
      }
    }
  } catch (e) { console.error('age chart error', e); }
}
