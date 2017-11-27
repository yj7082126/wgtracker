$(function(){
	$('#btnSignIn').click(function(){
		
		$.ajax({
			url: '/detail',
			data: $(this).text(),
			type: 'POST',
			success: function(response){
				console.log(response);
				document.write(response);
			},
			error: function(error){
				console.log(error);
				window.location.reload();
			}
		});
	});
});