$(document).ready(function () {
    $('#select_all').change(function (event) {
        $('input[type=checkbox]').prop('checked', event.target.checked);
    });
});
