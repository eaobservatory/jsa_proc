$(document).ready(function () {
    var connector = new samp.Connector('JSA Processing System',
                                       {
                                           'samp.name': 'JSAProc',
                                       },
                                       null,
                                       null);

    $(window).unload(function() {
        connector.unregister();
    });

    $('[id^="broadcast_"]').click(function (event) {
        var message = new samp.Message('image.load.fits', {
            'url': $(event.target).data('url'),
        });

        // Broadcasting code taken from the sampjs "broadcaster.js" example.
        // https://github.com/astrojs/sampjs/blob/gh-pages/examples/broadcaster.js
        var regSuccessHandler = function(conn) {
            connector.setConnection(conn);
            conn.notifyAll([message]);
        };

        var registerAndSend = function() {
            samp.register(connector.name, regSuccessHandler, null);
        };

        if (connector.connection) {
            connector.connection.notifyAll([message], null, registerAndSend);
        }
        else {
            registerAndSend();
        }

        event.preventDefault();
    });

    setInterval(function () {
        samp.ping(function (isActive) {
            $('[id^="broadcast_"]').prop('disabled', ! isActive);
        });
    }, 2000);
});
