$('label').on('click', function() {
    $(this).toggleClass('wrap');
});

$("#admin-button").click(function() {
    $("#newpost").fadeToggle(150);
});

$("#filter_date_start").keyup(function(event) {
    if(event.keyCode == 13) {
        $("#btn_action_filter_interval").click();
    }
});
