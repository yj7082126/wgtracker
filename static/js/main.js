$(document).ready(function(){
	$('#dailygroup li').click(function(){
		$.ajax({
			url: '/detail',
			data: $(this).text(),
			type: 'POST',
			success: function(response){
				console.log(response);
			},
			error: function(error){
				console.log(error);
			}
		});
	});
	
	$('#inputDate').click(function() {
 
        $.ajax({
            url: '/newDate',
            data: $('form').serialize(),
            type: 'POST',
            success: function(response) {
                console.log(response);
            },
            error: function(error) {
                console.log(error);
            }
        });
    });
});