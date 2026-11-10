/**
 * ==============================================================================
 * HEATMAP.JS — THUẬT TOÁN MA TRẬN NHIỆT MẬT ĐỘ NHU CẦU 24 GIỜ × 7 NGÀY
 * ==============================================================================
 */

document.addEventListener("DOMContentLoaded", () => {
  renderHeatmap();
});

/* ==============================================================================
 * HÀM RENDER MA TRẬN NHIỆT (RENDER HEATMAP TABLE)
 * ============================================================================== */
window.renderHeatmap = function() {
  const container = document.getElementById('heatmapWrapper');
  if (!container) return;

  const data = window.NYC_DATA || window.NYC_CURRENT_DATA || window.NYC_REAL_DATA;
  const matrix = data && data.heatmap ? data.heatmap : null;

  if (!matrix) {
    container.innerHTML = `
      <div style="padding: 32px; text-align: center; color: var(--text-muted); font-size: 13.5px;">
        ⏳ Đang kết nối cơ sở dữ liệu Hive SQL và tổng hợp ma trận nhiệt...
      </div>
    `;
    return;
  }

  // Sắp xếp theo tuần làm việc: Thứ Hai -> Chủ Nhật
  // Quy ước Spark DAYOFWEEK: 1 = Chủ Nhật, 2 = Thứ Hai, ..., 7 = Thứ Bảy
  const daysConfig = [
    { name: 'Thứ Hai', sparkIdx: 1 },
    { name: 'Thứ Ba', sparkIdx: 2 },
    { name: 'Thứ Tư', sparkIdx: 3 },
    { name: 'Thứ Năm', sparkIdx: 4 },
    { name: 'Thứ Sáu', sparkIdx: 5 },
    { name: 'Thứ Bảy', sparkIdx: 6 },
    { name: 'Chủ Nhật', sparkIdx: 0 }
  ];

  // 1. Tìm giá trị lớn nhất trong toàn bộ ma trận để chuẩn hóa thang màu
  let maxVal = 1;
  for (let d = 0; d < 7; d++) {
    for (let h = 0; h < 24; h++) {
      if (matrix[d] && matrix[d][h] > maxVal) {
        maxVal = matrix[d][h];
      }
    }
  }

  // 2. Tạo tiêu đề các cột: 24 khung giờ
  let html = '<table class="heatmap-table"><thead><tr><th style="min-width: 90px; text-align: left; padding-left: 10px;">Thứ / Giờ</th>';
  for (let h = 0; h < 24; h++) {
    html += `<th>${h}h</th>`;
  }
  html += '</tr></thead><tbody>';

  // 3. Render từng hàng thứ và 24 ô giờ
  daysConfig.forEach(day => {
    html += `<tr><td class="day-label">${day.name}</td>`;
    const rowData = matrix[day.sparkIdx] || [];

    for (let h = 0; h < 24; h++) {
      const val = rowData[h] || 0;
      const intensity = Math.min(1, Math.max(0, val / maxVal));

      // Thang màu trực quan:
      // Xanh dương nhẹ -> Hổ phách -> Đỏ san hô rực rỡ lúc cao điểm
      let bg = `rgba(56, 189, 248, ${0.12 + intensity * 0.75})`;
      let textColor = '#cbd5e1';

      if (intensity > 0.7) {
        bg = `rgba(244, 63, 94, ${0.7 + intensity * 0.3})`;
        textColor = '#ffffff';
      } else if (intensity > 0.4) {
        bg = `rgba(245, 158, 11, ${0.6 + intensity * 0.4})`;
        textColor = '#ffffff';
      }

      // Rút gọn hiển thị dạng 12.5k
      const displayVal = val >= 1000 ? `${(val / 1000).toFixed(1)}k` : `${val}`;

      html += `
        <td class="heatmap-cell"
            style="background: ${bg}; color: ${textColor};"
            title="${day.name} lúc ${h}h00: ${Number(val).toLocaleString('vi-VN')} chuyến (${(intensity * 100).toFixed(0)}% tải đỉnh)">
          ${displayVal}
        </td>
      `;
    }
    html += '</tr>';
  });

  html += '</tbody></table>';
  container.innerHTML = html;
};
