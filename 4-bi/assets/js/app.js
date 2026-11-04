/**
 * app.js
 * ------
 * Quản lý điều hướng tab và tương tác trên Dashboard.
 */

function switchTab(index) {
  document.querySelectorAll('.tab-btn').forEach((btn, i) => {
    btn.classList.toggle('active', i === index);
  });
  document.querySelectorAll('.tab-content').forEach((content, i) => {
    content.classList.toggle('active', i === index);
  });
}
