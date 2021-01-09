$(document).ready(function () {
    $('a.show_more_link').click(function (event) {
        event.preventDefault();
        var this_row = $(this).closest('tr');
        this_row.prevUntil(':visible').show();
        this_row.hide();
    });
});
