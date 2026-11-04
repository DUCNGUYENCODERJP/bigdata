/**
 * heatmap.js
 * ----------
 * Thuật toán tạo Ma trận nhiệt Nhu cầu đi xe (24 Giờ x 7 Ngày trong tuần).
 * Quy ước Spark: 1 = Chủ Nhật, ..., 7 = Thứ Bảy.
 */

document.addEventListener("DOMContentLoaded", () => {
  renderHeatmap();
});

function renderHeatmap() {
  const heatmapWrapper = document.getElementById('heatmapWrapper');
  if (!heatmapWrapper) return;

  const dayNames = [
    'Chủ Nhật (1)', 'Thứ Hai (2)', 'Thứ Ba (3)',
    'Thứ Tư (4)', 'Thứ Năm (5)', 'Thứ Sáu (6)', 'Thứ Bảy (7)'
  ];

  let tableHtml = '<table class="heatmap-table"><thead><tr><th>Thứ / Giờ</th>';
  for (let h = 0; h < 24; h++) {
    tableHtml += `<th>${h}h</th>`;
  }
  tableHtml += '</tr></thead><tbody>';

  const baseH = [
    32, 21, 14, 9, 8, 12, 31, 68, 95, 88, 76, 78,
    82, 84, 86, 89, 92, 115, 122, 118, 105, 94, 78, 52
  ];
  const dayMult = [0.85, 0.95, 1.0, 1.05, 1.15, 1.30, 1.10];

  dayNames.forEach((dName, dIdx) => {
    tableHtml += `<tr><td style="text-align: left; font-weight: 600; color: #cbd5e1; background: #1e293b;">${dName}</td>`;
    for (let h = 0; h < 24; h++) {
      let val = Math.round(baseH[h] * dayMult[dIdx] * (0.95 + 0.1 * Math.sin(h)));
      if ((dIdx === 5 || dIdx === 6) && (h >= 21 || h <= 2)) val = Math.round(val * 1.35);
      if (dIdx === 0 && h >= 5 && h <= 9) val = Math.round(val * 0.65);

      // Thang màu mật độ (Color scale)
      let intensity = Math.min(1, Math.max(0, (val - 8) / 150));
      let bg = `rgba(59, 130, 246, ${0.15 + intensity * 0.85})`;
      if (intensity > 0.75) bg = `rgba(239, 68, 68, ${0.7 + intensity * 0.3})`;
      else if (intensity > 0.5) bg = `rgba(245, 158, 11, ${0.6 + intensity * 0.4})`;

      tableHtml += `<td class="heat-cell" style="background: ${bg}; color: #fff;" title="${dName} lúc ${h}h: ${val * 1000} chuyến">${val}k</td>`;
    }
    tableHtml += '</tr>';
  });
  tableHtml += '</tbody></table>';
  heatmapWrapper.innerHTML = tableHtml;
}
