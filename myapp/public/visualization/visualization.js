function day_ahead_line_plot(x,y, vecm_y, var_y){

    // Allow two line next to each other
    var foo = [];
    for (var i = 1; i <= 23; i++) {
       foo.push(null);
    }

    console.log("x: ",x);
    console.log("y: ",y);
    console.log("vecm: ",vecm_y);
    console.log(foo);

    dataset_list = [{
        label: 'Current Price',
        lineTension: 0,
        fill: false,
        borderColor: "#F1D962",
        backgroundColor: "#F1D962",
        pointBackgroundColor: "#E0C020",
        pointBorderColor: "#E0C020",
        pointHoverBackgroundColor: "#E0C020",
        pointHoverBorderColor: "#E0C020",
        data: y,
    }]

    if(var_y !== undefined){
        var_y = [y[y.length - 1]].concat(var_y)
        var_y = foo.concat(var_y)

        dataset_list.push({
            label: 'VAR',
            lineTension: 0,
            fill: false,
            borderColor: "#9bff7f",
            backgroundColor: "#acff7f",
            pointBackgroundColor: "#75e755",
            pointBorderColor: "#6be755",
            pointHoverBackgroundColor: "#6be755",
            pointHoverBorderColor: "#75e755",
            data: var_y,
        })

    }
    /*
    if(sarima_y !== undefined){
        console.log("SARIMA: ",sarima_y);
        sarima_y = [y[y.length - 1]].concat(sarima_y)
        console.log("Unshift sarima: ",sarima_y)
        sarima_y = foo.concat(sarima_y)
        y = y.concat(sarima_y[0]).concat(foo)

        dataset_list.push({
            label: 'SARIMA',
            lineTension: 0,
            fill: false,
            borderColor: "#ff7f7f",
            backgroundColor: "#ff7f7f",
            pointBackgroundColor: "#e75555",
            pointBorderColor: "#e75555",
            pointHoverBackgroundColor: "#e75555",
            pointHoverBorderColor: "#e75555",
            data: sarima_y,
        })

    }*/
    if(vecm_y !== undefined){
        vecm_y = [y[y.length - 1]].concat(vecm_y)
        vecm_y = foo.concat(vecm_y)

        dataset_list.push({
            label: 'VECM',
            lineTension: 0,
            fill: false,
            borderColor: "#ff7f7f",
            backgroundColor: "#ff7f7f",
            pointBackgroundColor: "#e75555",
            pointBorderColor: "#e75555",
            pointHoverBackgroundColor: "#e75555",
            pointHoverBorderColor: "#e75555",
            data: vecm_y,
        })

    }

    var ctx = document.getElementById('day_ahead_line_plot').getContext('2d');
    ctx.canvas.width = 300;
    ctx.canvas.height = 500;
    var myChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: x,
            datasets: dataset_list
        },
        options: {
            tooltips: {
                callbacks: {
                    label: function (tooltipItem, data) {
                        currentLabel = data.datasets[tooltipItem.datasetIndex].label
                        return currentLabel + " : " + tooltipItem.yLabel.toPrecision(4);
                    }
                }
            },
            title:{
                display: true,
                fontSize: 30,
                text: "Day-Ahead Prices"
            },
            maintainAspectRatio: false,
            scales: {
                xAxes: [{
                    ticks:{
                        callback: function(time){
                            if(Number.isInteger(time)){
                                return time
                            }else{
                                return parseInt(time.split(" ")[1])
                            }
                        }
                    },
                    scaleLabel: {
                        display: true,
                        labelString: 'Time (Hours)'
                    }
                }],
                yAxes: [{
                    ticks: {
                        beginAtZero: true,
                        suggestedMax: Math.max.apply(Math, y) + 3
                    },
                    scaleLabel: {
                        display: true,
                        labelString: 'EUR / MWh'
                    }
                }]
            }
        }
    });
}

function isCanvasBlank(canvas) {
    return !canvas.getContext('2d')
        .getImageData(0, 0, canvas.width, canvas.height).data
        .some(channel => channel !== 0);
}