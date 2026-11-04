/**
 * charts.js
 * ---------
 * Khởi tạo toàn bộ các biểu đồ Chart.js:
 * 1. Thị phần theo quận (chartBorough)
 * 2. Top 8 khu vực đón xe đông nhất (chartTopZones)
 * 3. Biến động giá cước & thời gian theo 24 giờ (chartHourlyCombo)
 * 4. Cuốc xe bị hủy theo giờ (chartCancelledHours)
 * 5. Tỷ lệ cuốc hủy theo năm (chartCancelledYears)
 * 6. Chuỗi thời gian so sánh dự báo ML (chartForecast)
 */

document.addEventListener("DOMContentLoaded", () => {
  initBoroughChart();
  initTopZonesChart();
  initHourlyComboChart();
  initCancelledCharts();
  initForecastChart();
});

// 1. Chart: Borough Share
function initBoroughChart() {
  const ctx = document.getElementById('chartBorough');
  if (!ctx) return;
  new Chart(ctx.getContext('2d'), {
    type: 'doughnut',
    data: {
      labels: ['Manhattan', 'Queens (Sân bay JFK/LGA)', 'Brooklyn', 'Bronx', 'Staten Island'],
      datasets: [{
        data: [82.4, 11.2, 5.1, 1.0, 0.3],
        backgroundColor: ['#3b82f6', '#f59e0b', '#10b981', '#8b5cf6', '#64748b'],
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { color: '#cbd5e1', font: { size: 12 } } }
      }
    }
  });
}

// 2. Chart: Top Zones
function initTopZonesChart() {
  const ctx = document.getElementById('chartTopZones');
  if (!ctx) return;
  new Chart(ctx.getContext('2d'), {
    type: 'bar',
    data: {
      labels: [
        'Upper East South', 'Midtown Center', 'Upper East North',
        'Times Sq/Theatre', 'Lincoln Sq East', 'JFK Airport',
        'Murray Hill', 'Penn Station'
      ],
      datasets: [{
        label: 'Lưu lượng chuyến (Triệu lượt)',
        data: [14.2, 13.8, 12.9, 11.5, 10.4, 9.8, 8.9, 8.5],
        backgroundColor: '#3b82f6',
        borderRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } },
        y: { grid: { display: false }, ticks: { color: '#f8fafc' } }
      }
    }
  });
}

// 3. Chart: Hourly Combo (Fare & Duration)
function initHourlyComboChart() {
  const ctx = document.getElementById('chartHourlyCombo');
  if (!ctx) return;
  new Chart(ctx.getContext('2d'), {
    type: 'line',
    data: {
      labels: Array.from({ length: 24 }, (_, i) => `${i}h`),
      datasets: [
        {
          label: 'Giá cước TB (USD)',
          data: [18.4, 19.2, 20.5, 21.0, 22.5, 20.8, 17.5, 16.2, 16.8, 17.1, 16.5, 16.2, 16.4, 16.9, 17.5, 18.2, 18.8, 19.5, 19.2, 18.4, 17.8, 17.5, 17.9, 18.1],
          borderColor: '#f59e0b',
          backgroundColor: 'transparent',
          yAxisID: 'yFare',
          tension: 0.3,
          borderWidth: 3
        },
        {
          label: 'Thời gian di chuyển TB (phút)',
          data: [12.5, 12.0, 11.8, 11.5, 12.2, 13.5, 15.8, 19.2, 21.5, 20.2, 18.5, 17.8, 18.2, 18.6, 19.4, 21.2, 22.8, 23.5, 22.1, 19.8, 17.5, 16.2, 15.0, 13.8],
          borderColor: '#3b82f6',
          backgroundColor: 'rgba(59, 130, 246, 0.1)',
          fill: true,
          yAxisID: 'yDuration',
          tension: 0.3,
          borderWidth: 3
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#cbd5e1' } } },
      scales: {
        x: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } },
        yFare: {
          type: 'linear', position: 'left',
          grid: { color: '#334155' }, ticks: { color: '#f59e0b', callback: v => `$${v}` },
          title: { display: true, text: 'Giá cước (USD)', color: '#f59e0b' }
        },
        yDuration: {
          type: 'linear', position: 'right',
          grid: { display: false }, ticks: { color: '#3b82f6', callback: v => `${v}p` },
          title: { display: true, text: 'Thời gian (Phút)', color: '#3b82f6' }
        }
      }
    }
  });
}

// 4. Charts: Cancelled Trips
function initCancelledCharts() {
  const ctxHour = document.getElementById('chartCancelledHours');
  if (ctxHour) {
    new Chart(ctxHour.getContext('2d'), {
      type: 'bar',
      data: {
        labels: ['0h', '4h', '8h', '12h', '17h', '18h', '19h', '21h', '23h'],
        datasets: [{
          label: 'Cuốc bị hủy',
          data: [1420, 890, 2100, 2800, 4950, 5400, 5100, 4200, 3100],
          backgroundColor: '#ef4444',
          borderRadius: 6
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } },
          y: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } }
        }
      }
    });
  }

  const ctxYear = document.getElementById('chartCancelledYears');
  if (ctxYear) {
    new Chart(ctxYear.getContext('2d'), {
      type: 'bar',
      data: {
        labels: ['2023', '2024', '2025 (Bất thường TLC)'],
        datasets: [{
          label: 'Tỷ lệ cuốc fare âm (%)',
          data: [0.35, 0.42, 8.45],
          backgroundColor: ['#64748b', '#64748b', '#ef4444'],
          borderRadius: 6
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { color: '#334155' }, ticks: { color: '#f8fafc' } },
          y: { grid: { color: '#334155' }, ticks: { color: '#94a3b8', callback: v => `${v}%` } }
        }
      }
    });
  }
}

// 5. Chart: ML Forecast Time Series
function initForecastChart() {
  const ctx = document.getElementById('chartForecast');
  if (!ctx) return;

  const timeLabels = [
    '20/01 08h', '20/01 12h', '20/01 18h', '20/01 22h',
    '21/01 08h', '21/01 12h', '21/01 18h', '21/01 22h',
    '22/01 08h', '22/01 12h', '22/01 18h', '22/01 22h',
    '23/01 08h', '23/01 12h', '23/01 18h', '23/01 22h'
  ];

  new Chart(ctx.getContext('2d'), {
    type: 'line',
    data: {
      labels: timeLabels,
      datasets: [
        {
          label: 'Nhu cầu thực tế (Actual Label)',
          data: [180, 160, 245, 190, 185, 165, 250, 195, 175, 155, 235, 180, 210, 190, 290, 230],
          borderColor: '#ffffff',
          backgroundColor: 'transparent',
          borderWidth: 2,
          borderDash: [5, 5],
          pointRadius: 4
        },
        {
          label: 'Dự báo GBTRegressor (Spark ML)',
          data: [175, 158, 238, 186, 182, 162, 244, 191, 172, 153, 230, 177, 205, 186, 282, 225],
          borderColor: '#10b981',
          backgroundColor: 'transparent',
          borderWidth: 3,
          pointRadius: 4,
          tension: 0.3
        },
        {
          label: 'Dự báo Baseline (Mean lịch sử)',
          data: [150, 140, 190, 160, 150, 140, 190, 160, 150, 140, 190, 160, 150, 140, 190, 160],
          borderColor: '#94a3b8',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 3,
          tension: 0.1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'top', labels: { color: '#cbd5e1', font: { size: 13 } } }
      },
      scales: {
        x: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } },
        y: {
          grid: { color: '#334155' },
          ticks: { color: '#94a3b8' },
          title: { display: true, text: 'Số lượng chuyến / zone / giờ', color: '#cbd5e1' }
        }
      }
    }
  });
}
