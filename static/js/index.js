const ctx = document.getElementById('myChart').getContext('2d')
var chartData =  {
    type: 'bar',
    data: {
        labels: ['Meta', 'Amazon', 'Netflix', 'Google', 'Apple'],
        datasets: [{
            label: '# of Votes',
            data: [0, 0, 0, 0, 0, 0],
            backgroundColor: [
                'rgba(255, 99, 132, 0.2)',
                'rgba(54, 162, 235, 0.2)',
                'rgba(255, 206, 86, 0.2)',
                'rgba(75, 192, 192, 0.2)',
                'rgba(153, 102, 255, 0.2)',
                'rgba(255, 159, 64, 0.2)'
            ],
            borderColor: [
                'rgba(255, 99, 132, 1)',
                'rgba(54, 162, 235, 1)',
                'rgba(255, 206, 86, 1)',
                'rgba(75, 192, 192, 1)',
                'rgba(153, 102, 255, 1)',
                'rgba(255, 159, 64, 1)'
            ],
            borderWidth: 1
        }]
    },
    options: {
        indexAxis: 'y',
        scales: {
            y: {
              ticks: { color: '#6c7293', beginAtZero: true }
            },
            x: {
              ticks: { color: '#6c7293', beginAtZero: true }
            }
          },
          plugins: {
            legend: {
                labels: {
                    // This more specific font property overrides the global property
                    font: {
                        size: 16
                    }
                }
            }}
    }
}
var myChart = new Chart(ctx, chartData);

setInterval(function(){
    $.getJSON('/refreshData', function(data){
        {
            chartData.data.datasets[0].data = [parseInt(data.meta), parseInt(data.amz), parseInt(data.netflix), parseInt(data.gg), parseInt(data.apple)]
            console.log(data.netflix)
            myChart.update()
        }
    })
} ,2000);