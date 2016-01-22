<?php  //crm-login-2.php
include 'inc/sitefunctions.php';
require_once 'vendor/autoload.php';
?>

<!DOCTYPE html>
<html>

<head>
<meta content="text/html; charset=utf-8" http-equiv="Content-Type" />
<title>Watoto Hong Kong CRM</title>
<style>

div#wrapper {
	position: relative;
		left:30%;
		top:100px;
		width:40%;
		/* border: 1px blue solid;
		height:10vh;
		overflow: scroll;    
			 */
}

div#wrapper table {
	width: 100%;
	border: 1px solid silver  ; 
	/* background-color:olive ; */
}


</style>
</head>

<body>
<?php 

$USERS = array( 
	'christyma' => array ( 'christy123', array('FinanceTeam', 'UserAdmin') ) ,
	'nicol' => array ('nicol123' , array('SponsorshipTeam') ) ,
	'ison' => array ('ison123', array('VisitwatotoTeam') ) ,
	'joanne' => array ('joanne123', array('ChoirTeam', 'VisitwatotoTeam') ),
	'dummy' => array ('dummy123', array() ) 
);

$ROLES = array('FinanceTeam', 'SponsorshipTeam', 'VisitwatotoTeam', 'ChoirTeam', 'UserAdmin'); 

$matched_role = ""; 

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
	if (isset($_POST['username'], $_POST['password']) ) {
		extract($_POST, EXTR_PREFIX_ALL, 'tmp') ;
		if ( array_key_exists($tmp_username, $USERS) &&  $tmp_password == $USERS[$tmp_username][0]) {

			// Login is validated. Check role(s) of user
			$matched_role = array_intersect($ROLES, $USERS[$tmp_username][1]);          
			if (empty($matched_role)) {
						 echo 'User has no valid role.';
			} else {
				// Create a PHP user session
				session_start();
				$_SESSION['username'] = $tmp_username;
				$_SESSION['roles'] = $USERS[$tmp_username][1]; 
				session_write_close();
				header("Location:/menu-3.php");
				exit; 
			}

		} else {
				// Login fail
				echo 'Invalid login. Please try again.' ;
				// movePage(401, 'http://192.168.1.103/messages/error401.php');
				// exit;
		}
	} 
	else {
			echo 'Invalid login. Please try again.' ;
	}
}
?>

<h1>  Watoto Hong Kong </h1>
<hr>
<div id="wrapper">
<form action="crm-login-3.php" method="post">
<fieldset> <legend> CRM LOGIN </legend>
<table> 
		<thead></thead>
<tbody>
<tr> 
	 <td> <label for="username"> Login Name: </label> </td>  
	 <td align="right">  <input id="username" type="text" name="username" size="30"/> </td>
</tr>
<tr>  
	 <td> <label for="password">Password:   </label> </td> 
	 <td align="right"> <input id="password" type="password" name="password" size="30"  />   </td>

</tr>
<tr> 
	 <td colspan="2" align="right"><input type="submit" value="Login" /> </td>
</tr>
</tbody>
		 <tfoot></tfoot>
</table>

</fieldset>
</form>

</div>
</body>

</html>
