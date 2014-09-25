$(document).ready(function () {
    $('#select_all').change(function (event) {
      checkboxes = document.getElementsByName('job_id');
      for(var i=0, n=checkboxes.length;i<n;i++) {
        checkboxes[i].checked = event.target.checked;
      }
    });
});
