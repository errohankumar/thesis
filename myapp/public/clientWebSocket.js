var socket = io.connect();
$(document).ready(function () {
    function webSocketInvoke() {

        /*var socket = io();*/

        socket.on("message", function (data) {
            console.log("data.key: ", data.key);
            console.log("data.value: ", JSON.parse(data.value));

            data_key = data.key
            data_value = JSON.parse(data.value)

            if (data_key === "Dataset") {
                x = data_value.timestamps
                day_y = data_value.day_ahead_price

            }else if(data_key === "VAR"){
                console.log(data_key)
                var_y = data_value.forecast
                var_delivery_date = data_value.delivery_date
                var_error = data_value.mean_abs_error
                console.log("VAR delivery date: ", var_delivery_date)
                if (typeof sarima_y !== "undefined" && var_delivery_date !== sarima_delivery_date){
                    day_ahead_line_plot(x, day_y, undefined, var_y);
                }
            }
            /*else if (data_key === "SARIMA") {     // '===' is used in JS to asks if the right operand is the same as the left operand.
                console.log(data_key)
                sarima_y = data_value.forecast
                sarima_delivery_date = data_value.delivery_date
                sarima_error = data_value.mean_abs_error
                console.log("SARIMA delivery date: ", sarima_delivery_date)
                if (typeof day_y !== 'undefined') {
                    day_ahead_line_plot(x, day_y, sarima_y, var_y);
                    }
                }*/
              else if (data_key === "VECM") {
                 console.log(data_key)
                 vecm_y = data_value.forecast
                 vecm_delivery_date = data_value.delivery_date
                 vecm_error = data_value.mean_abs_error
                 console.log("VECM delivery date: ", vecm_delivery_date)
                 console.log(vecm_y)
                if (typeof day_y !== 'undefined') {
                    day_ahead_line_plot(x, day_y, vecm_y, var_y);
                }
            }

        });

        socket.on("disconnect", function (data) {
            console.log("Client WebSocket Disconnected: ", data)
            socket.disconnect();
        });
    }
        webSocketInvoke();
});