<?php

set_time_limit(0);


/**************************** Environement Settings ******************************************/

include_once ("config/db.inc.php");
include_once ("lib/getwebroot.php");
include_once ("lib/converter.php");
include_once ("lib/util.php");
include_once ("lib/array_to_xml.php");
include_once ("lib/class.array_to_xml.php");
include_once ("lib/xml_to_array.php");
include_once ("/etc/ufosend/edm_servers-sd.php");
libxml_use_internal_errors(true);


/***************************** Library Functions *****************************************/

// function _trigger_edm ($file_order, $send_size, $available_servers, $distributed_servers, $from, &$send_hash) {
function _trigger_edm ($file_order, $from, &$send_hash) {

   global $cobrand;

   /***************** Re-initalize Param (for readiness convenience only) *********************************/

   // $hostname = $send_hash['hostname'];
   // $campaign_id = $send_hash['campaign_id'];
   // $batch_id = $send_hash['batch_id'];
   // $rs = $send_hash['seq_num'];
   // $sp_id = $send_hash['sp_id'];
   // $post_datetime = $send_hash['post_datetime'];
   // $random_string = $send_hash['random_string'];
   // $seq_num = $send_hash['seq_num'];
   // $mockup = $send_hash['mockup'];
   // $debug = $send_hash['debug'];
   // $db_name = $cobrand['db_cfg']['mysql_database'];

   $send_hash['db_name'] = $cobrand['db_cfg']['mysql_database'];
   $send_hash['sender'] = $from;
   $send_hash['encrypt'] = $cobrand['encrypt'];
   $send_hash['file_order'] = $file_order; // it's been looping, each dispatch is different


   /************************************** Process Send Out ****************************************/

   // get $send_hash['edm_server'], this is the best server return from _select_edm_server 
   // _select_edm_server also checks if edm server is alive or dead
   list($rc, $rm) = _select_edm_server ($send_hash);

   elog ("DB Name : " . $send_hash['db_name'] . ", Best eDM Server : " . $send_hash['edm_server'] . " ...");

   // get return path domain
   list ($rc, $rm, $send_hash['return_path_domain']) = get_option ('edm', 'EDM.returnPathDomain'); 

   // post to edm server
   list($rc, $rm) = _post_to_edm_server($send_hash);
 

   if ($rc) {

       $curl_datetime = date('Y-m-d H:i:s', mktime(date("H"), date("i"), date("s"), date("m"), date("d"),  date("Y")));

       $insert_sql = "INSERT INTO batch_breakdowns (`sp_id`, 
                                                    `campaign_id`, 
                                                    `batch_id`, 
                                                    `release_sequence`, 
                                                    `edm_server`, 
                                                    `curl_datetime`, 
                                                    `mockup`, 
                                                    `file_order`, 
                                                    `send_size`) 
                                            VALUES ('" . $send_hash['sp_id'] . "', 
                                                    '" . $send_hash['campaign_id'] . "',  
                                                    '" . $send_hash['batch_id'] . "',  
                                                    '" . $send_hash['release_sequence'] . "',  
                                                    '" . $send_hash['edm_server'] . "', 
                                                    '$curl_datetime', 
                                                    '" . $send_hash['mockup'] . "',  
                                                    '" . $send_hash['file_order'] . "',  
                                                    '" . $send_hash['campaign_id'] . "', 
                                                    '" . $send_hash['send_size'] . "')"; 

       // elog("Insert SQL (edm.inc.php::_trigger_edm) > insert : $insert_sql");
       $result = mysqli_query( $cobrand['link'], $insert_sql );

   }

   elog("Trigger RC (Batch ID : " . $send_hash['batch_id'] . " / File Order : " . $send_hash['file_order'] . ") : $rc , RM : $rm");

   return array ($rc, $rm);

}


// this is to distribute the release into different edm gateways
// e.g. if it has 10 gateways, and 3 of which has more IPs, this 3 have higher chance to get the task
// so, num_ip is the weight
function _select_edm_server (&$send_hash) {

    global $cobrand;

    elog ("*******************************************************");
    elog ("in select edm server ....");
    elog ("count : "  . count($send_hash['distributed_servers']) . "....");
    elog ("Available Servers : " . print_r($send_hash['available_servers'], true) );
    elog ("Distributed Servers (pass in) : " . print_r($send_hash['distributed_servers'], true) );

    if ( count($send_hash['distributed_servers']) == 0 ) {

	       $j = 1;

         foreach ($send_hash['available_servers'] as $server => $array) {

  	          // $num_ip = $available_servers[$server]['num_ip'];  // as weight
              $num_ip = $send_hash['available_servers'][$server]['num_ip'];  // as weight
      	      elog ("server : $server, num ip : $num_ip ....");

      	      for ( $i=1; $i <= $num_ip; $i++ ) {
    	      	      elog (">> i : $i ....");
                    $send_hash['distributed_servers'][$j] = $server; 
		                ++$j;
	            }

         }

    }

    elog ("Distributed Servers (before unset) : " . print_r($send_hash['distributed_servers'], true) );
    $index = array_rand($send_hash['distributed_servers']);
    elog ("Index : " . $index);
    $send_hash['edm_server'] = $send_hash['distributed_servers'][$index];
    // unset ($send_hash['distributed_servers'][$index]);
    elog ("*******************************************************");


    /******************* special assignment for edm servers ****************/
    elog ("Server Owner : " . $cobrand['server_owner']);

    // when test bulk, must use multiple servers
    if ( $cobrand['server_owner'] == 'ufosend' && $send_hash['mockup'] == 1 ) {  
         $send_hash['edm_server'] = 'inmartsmtp5.hkqmail.com';
    }

    if ( $send_hash['db_name'] == 'ufo_dev' ) {
         $send_hash['edm_server'] = 'smtp1m.ufosend.com';    // only hard code for development use
    }

    /*********************** check edm server is alive or dead  ***********************/
    /******* in case, remote server died, we force to assign using GW6 or GW7 *********/
    if ( _ping_edm_server ($send_hash['edm_server']) === FALSE ) {

         elog (">> here " . $send_hash['edm_server'] . " died");

         if ( $send_hash['edm_server'] != 'smtp1m.ufosend.com' ) {
              $send_hash['edm_server'] = 'smtp1m.ufosend.com';    // force to use GW6
         } else {
              $send_hash['edm_server'] = 'smtp2m.ufosend.com';    // if in case, GW6 itself died, force to use GW7
         }

    }     

    elog ("Best Server : " . $send_hash['edm_server']);

    // elog ("Distributed Servers (after unset) : " . print_r($distributed_servers, true) );

    return array (1, 'OK');

}


// Query Different Servers to Get the Best One to Send
function _get_edm_server_queues () {

    global $cobrand;

    $delegated_servers = array ();
    $available_servers = array ();

    // Get Delegated Servers
    list ($rc, $rm, $delegated_edm_servers) = get_option ('edm', 'EDM.delegatedEdmServers');
    elog ("delegated edm servers >> RC : $rc, RM : $rm, delegated_edm_servers : $delegated_edm_servers");

    list ($rc, $rm, $customer_grading) = get_option ('edm', 'EDM.customerGrading');
    $customer_grading = trim($customer_grading);

    // if not default, get delegated server numbers
    if ( $delegated_edm_servers != 'default' ) { $delegated_servers = explode(",", $delegated_edm_servers); }

    // Check qualified servers
    foreach ($cobrand['edm_servers'] as $edm_server => $array ) {

    	 // if ( $array['enabled'] ) {    // now no need check enabled, Nov 2015

    	      // if not default, only allow those defined in EDM.delegatedEdmServers
    	      if ( $delegated_edm_servers != 'default' && !in_array($array['server_num'], $delegated_servers) ) { continue; }

    	      // if using delegated servers, always overwrite and no need check below grading servers
    	      if ( $delegated_edm_servers == 'default' ) {
    		         if ( $array['grade'] != $customer_grading ) { continue; }
    	      }

            // since performance no good, try skip queue size  (16 Nov 2014)
            //list($rc, $rm, $edm_server, $queue_size) = _query_edm_server($edm_server);
            //elog ("EDM Server : $edm_server, RC : $rc, RM : $rm, Queue Size : $queue_size");
            //if ($rc) { $available_servers[$edm_server]['queue_size'] = $queue_size; }

            if ($rc) { $available_servers[$edm_server]['num_ip'] = $cobrand['edm_servers'][$edm_server]['num_ip']; }

    	 // }

       //echo $i;

    }

    return array (1, 'OK', $available_servers);

}


// having assigned this edm_server, here to check if alive before actual sending out thur this server
function _ping_edm_server ($edm_server) {

    global $cobrand;

    $fsock = fsockopen($edm_server, 80, $errno, $errstr, 8);
    elog ("Socket : $fsock");

    if ( !$fsock ) {
         return FALSE;
    } else {
         return TRUE;
    }

}

// Query Individaul Server
/*
function _query_edm_server ($edm_server) {

    global $cobrand;

    $bb_version = $cobrand['edm_servers'][$edm_server]['bb_version'];

    $cobrand['solr_ch'] = curl_init();
    $script = 'http://' . $edm_server . '/' . 'queuing_status_' . $bb_version . '.php';
    list($rc, $rm, $queue_size) = curl_call('edm', $cobrand['solr_ch'], 'string', $script, '');
    curl_close ($cobrand['solr_ch']);

    //elog ("_query_edm_server : RC : $rc, RM : $rm, Queue Size : $queue_size");

    if ( $rc && $queue_size != '' ) {
         return array (1, "OK", $edm_server, $queue_size);
    } else {
         return array (0, "No This Server or Invalid Queue", $edm_server, '');
    }

}
*/


// Post to eDM Server
// this will post to GW /var/www/trigger_edm_v2.php script
// function _post_to_edm_server ($best_server, $file_order, $hostname, $return_path_domain, $db_name, $from, $post_datetime, $random_string, $sp_id, $campaign_id, $batch_id, $use_member_id, $seq_num, $mockup, $debug) {

function _post_to_edm_server (&$send_hash) {

    global $cobrand;

    // $encrypt = $cobrand['encrypt'];
    $send_hash['release_sequence'] = $send_hash['seq_num'];
    if ( $send_hash['mockup'] ) { $send_hash['release_sequence'] = 0; }

    elog("Going to post to Newsletter Server : " . $send_hash['edm_server'] );

    $send_hash['version'] = $cobrand['edm_servers'][$send_hash['edm_server']]['bb_version'];
    // $send_hash['path'] = $cobrand['web_path'];

    // parameters that no need submit, we do a unset here 
    // unset($send_hash)
    // $send_hash['release_sequence'] = $send_hash['seq_num'];
    // $send_hash['encrypted'] = $cobrand['encrypt'];

    $script = 'http://' . $send_hash['edm_server'] . '/' . 'trigger_edm_' . $send_hash['version'] . '_dev.php';

    // get only useful send hash variables to post
    $post_send_hash = $send_hash;
    unset ($post_send_hash['available_servers']);
    unset ($post_send_hash['distributed_servers']);
    unset ($post_send_hash['superuser_login']);
    unset ($post_send_hash['available_lang']);
    unset ($post_send_hash['allow_duplicate_send']);
    unset ($post_send_hash['edm_content']);

    // some values must do urlencode first
    $post_send_hash['campaign_name'] = urlencode($post_send_hash['campaign_name']);
    $post_send_hash['edm_subject'] = urlencode($post_send_hash['edm_subject']);

    $param = 'send_hash=' . json_encode($post_send_hash);
    // $param = urlencode($param);
    // $param = addcslashes($param, '"\\/');
    // $param = str_replace('$', '\$', $param);  // escape also dollar sign

    // elog("post send hash param: " . $param);
        // $send_hash = str_replace('&', 'AND', $send_hash);  // escape also dollar sign


    // $param = "version=" . $bb_version . "&edm_server=$best_server&hostname=$hostname&rpd=$return_path_domain&cn=$db_name&sender=$from&post_datetime=$post_datetime&ranstr=$random_string&file_order=$file_order&sp_id=$sp_id&campaign_id=$campaign_id&batch_id=$batch_id&use_member_id=$use_member_id&release_sequence=$seq_num&encrypted=$encrypt&mockup=$mockup&path=" . $cobrand['web_path'] . "&debug=$debug";


    $cobrand['solr_ch'] = curl_init();
    // $script = 'http://' . $best_server . '/' . 'trigger_edm_' . $bb_version . '.php';
    list($rc, $rm, $rec_data) = curl_call('edm', $cobrand['solr_ch'], 'string', $script, $param);
    curl_close ($cobrand['solr_ch']);

    elog("UFO BB Version : " . $send_hash['version']);
    elog("Posted to Newsletter Server : " . $send_hash['edm_server']);
    elog("Script : " . $script);
    elog("Param : $param");

    return array ($rc, $rm);

}


function SendEDM ($send_hash, $edm_template, &$stat_figures, &$return_lists) {

   global $cobrand;

   $send_hash['is_trial'] = 0;

   // elog ("NOTE : stat_figures :\n" . print_r($stat_figures, true) . "\n\n");
   // elog ("NOTE : start of send edm : return list :\n" . print_r($return_lists, true) . "\n\n");

   foreach ($return_lists['incorrect_email_list'] as $key => $user ) {

            $prefixed_email = $user['prefixed_email'];
            $email = $user['email'];

   }


   $cobrand['encrypt'] = 0;
   $send_hash['post_datetime'] = date("YmdHis");

   $send_error = 0;
   $new_rm = '';
   $tmp_rm = '';

   // elog(" ... Available lang here ... ");
   // elog($edm_template['available_lang']);
   // elog(" ... edm hash merge ... ");
   // elog(print_r($send_hash['edm_merge'], true));


   if ( empty($edm_template['available_lang']) ) {
        $send_hash['available_lang'][0] = 'none';
   } else {
        $send_hash['available_lang'] = explode(',', $edm_template['available_lang']);   // force $send_hash['available_lang'] to become an array
   }


   //elog(" Template AL : " . print_r($edm_template['available_lang'], true) );
   //elog(" AL : " . print_r($send_hash['available_lang'], true) );
   //elog(" AL Count : " . count($send_hash['available_lang']) );

   // loop for all available lang and validate content of each of the lang.
   foreach ( $send_hash['available_lang'] as $c_lang ) {

       //elog(" ... C Language : $c_lang");
       $content_field = 'content';
       if ( $c_lang != 'none' )  { $content_field .= '_' . $c_lang; }
       //if ( $c_lang != 'none' )  { $subject_field .= '_' . $c_lang; }

       //elog("edm subject : " . $edm_template[$subject_field]);
       list ($rc, $rm) = _validate_edm_format ($edm_template[$content_field]);
       if ( !$rc ) { return array ($rc, $rm); }

   }

   elog("\n\n");
   

   $account_hash = get_account_info('edm');
   if ( $account_hash['edm_service_plan'] == 'TRI' ) {  $send_hash['is_trial'] = 1;  }


   /************************* Get Latest Release / Mockup Sequence ************/
   if ( $send_hash['mockup'] ) { 
	      $send_hash['seq_field'] = 'mockup_sequence'; 
   } else { 
	      $send_hash['seq_field'] = 'release_sequence';
   }

   $get_sequence_sql = "SELECT MAX(" . $send_hash['seq_field'] . ") 
                            AS seq_num FROM batches 
                         WHERE campaign_id = '" . $send_hash['campaign_id'] . "'";
   $get_sequence_result = mysqli_query($cobrand['link'], $get_sequence_sql);
   $seq_row = mysqli_fetch_assoc($get_sequence_result);

   $send_hash['seq_num'] = $seq_row['seq_num'] + 1;


   /************************* Gen Temp. Files *************************/
   $send_hash['random_string'] = gen_random_string ();

   list($rc, $rm, $breakdown_array) = _edm_gen_tmp_files ($send_hash, $edm_template, $return_lists['sendout_email_list']);
   if ( !$rc ) { return array ($rc, $rm); }
  
   $num_files = count($breakdown_array);


   /************************* Write to edm_stats ************************/
   elog("Write to eDM Stats Start ... ");
   list ($rc, $rm) = _write_to_edm_stats ($send_hash, $return_lists);
   elog("Write to eDM Stats End ... ");

   elog("Seq Field : " . $send_hash['seq_field'] . " ... ");
   elog("Seq Num : " . $send_hash['seq_num'] . " ... ");


   /************************* Finalize to Send *****************************/
   list ($rc, $rm, $send_hash['seq_num']) = _edm_finalize ($send_hash, $stat_figures, $num_files);
   if ( !$rc ) { return array ($rc, $rm); }
   list ($rc, $rm, $send_hash['sp_id']) = update_send_progresses ('edm', 'sendout', $send_hash);  // send progress id


   // Get Server Queues
   elog("*************************************************************");
   elog("Get eDM Server Queue Start ... ");
   list ($rc, $rm, $send_hash['available_servers']) = _get_edm_server_queues ();
   elog("Get eDM Server Queue End ... ");
   elog("*************************************************************\n\n");


   // Trigger Solr to sync, so that frontend doesn't need to wait long to have waiting feedbacks figure
   $involved_collections = array ('edm_stats');
   list ($rc, $rm) = trigger_solr_client('edm', $cobrand['db_cfg']['mysql_database'], 
                                         $send_hash['campaign_id'], $send_hash['batch_id'], 
                                         $involved_collections, 1, 'sending edm to blackbox', 'instant_kick');


   // Trigger Send eDM
   $send_hash['distributed_servers'] = array();

   for ($i = 1; $i <= $num_files; $i++) {

        // list ($rc, $tmp_rm, $available_servers, $distributed_servers) = _trigger_edm ($i, $breakdown_array[$i], $available_servers, $distributed_servers, $edm_template['from'], $send_hash);
        // list ($rc, $tmp_rm) = _trigger_edm ($i, $breakdown_array[$i], $edm_template['from'], $send_hash);
        $send_hash['send_size'] = $breakdown_array[$i];
        list ($rc, $tmp_rm) = _trigger_edm ($i, $edm_template['from'], $send_hash);

        // $rm = _trigger_sms ($send_hash, $i, $sms_template['from']);

        if ( !$rc ) { $send_error++; }
        $new_rm .= $tmp_rm . "<BR>";

   }

   // if ( $send_hash['mockup'] ) { $rs = 0; } else { $rs = $send_hash['seq_num']; }

   if ( $send_error > 0 ) {
        return array (0, $new_rm, '');
   } else {
        return array (1, $new_rm, $send_hash['seq_num']);
   }

}


function _edm_gen_tmp_files (&$send_hash, $edm_template, $sendout_email_list) {

     global $cobrand;

     $breakdown_array = array();

     $folder = $cobrand['server_webroot'] . $cobrand['server_path'] . 'emarketing/tmp/';
     elog ("Folder : $folder");

     $edm_file = $cobrand['db_cfg']['mysql_database'] . '-' . $send_hash['post_datetime'] . '-' . $send_hash['random_string'] . '_edm_msg.txt';
     $local_edm_file = $folder . $edm_file;


     // eDM File
     // Trim Out Control Code VT (Vertical Tab)
     elog ("send hash available lang : " . print_r($send_hash['available_lang'], true) . "\n");

     foreach ( $send_hash['available_lang'] as $c_lang ) {

          elog ("C Lang : $c_lang\n");
          $content_field = 'content';
          $ptext_content_field = 'plain_text_content';
          $headline_field = 'headline';

          if ( $c_lang != 'none' )  { 
        	     $content_field .= '_' . $c_lang; 
        	     $ptext_content_field .= '_' . $c_lang; 
        	     $headline_field .= '_' . $c_lang; 
  	      }


         	$edm_template[$content_field] = preg_replace("/\v/", '', $edm_template[$content_field]);
         	$edm_template[$ptext_content_field] = preg_replace("/\v/", '', $edm_template[$ptext_content_field]);

         	elog ("headline field : $headline_field\n");
         	elog ("headline : " . $edm_template[$headline_field]);
         	//elog ("rss items : " . print_r($edm_template['rss_items'], true));

          if ( isset($edm_template['rss_items']) ) { 

  	           $edm_template[$content_field] = loop_and_merge_rss_content ('edm', $send_hash, $edm_template['rss_items'], $edm_template[$content_field]);
       	        unset($edm_template['rss_items']);

          }

     }


     $xmlobj = new ArrayToXML();
     $xml1 = $xmlobj->buildXMLData($edm_template, 'edm');

     $handle1 = fopen ($local_edm_file, "w");
     if ( !$handle1 ) { return array (0, 'Cannot create file in /emarketing/tmp folder'); }

     //elog ("xml1 \n$xml1");
     fwrite($handle1, $xml1);
     fclose($handle1);

     // User File, Encrypt Data as Well
     // Also Break Into Pieces for Connecting to Different Servers

     $count = count($sendout_email_list);

     $i = 0;
     $boundry = 0;
     $limit = 3000;

     while (++$i) {

            if ( $boundry >= $count ) { break; }

            $output = 'output' . $i;
            $$output = array_slice($sendout_email_list, $boundry, $limit);

            $user_file = $cobrand['db_cfg']['mysql_database'] . '-' . $send_hash['post_datetime'] . '-' . $send_hash['random_string'] . '_edm_users-' . $i . '.txt';
            $local_user_file = $folder . $user_file;
            $handle2 = fopen ($local_user_file, "w");
            $xml = arrayToXml($$output);
      	    
            if ( $cobrand['encrypt'] == 1 ) {
                 $xml = encrypt_data('edm', $xml);
      	    }
            
            fwrite($handle2, $xml);
            fclose($handle2);

            $boundry = $i * $limit;
            $breakdown_array[$i] = count($$output);

     }

     //exit;
     //$num_files = $i-1;
     //elog("in _gen_tmp_files, I : $num_files");
     //return array (1, 'OK', $num_files);

     return array (1, 'OK', $breakdown_array);

}


function _validate_edm_format ($content) {

   global $cobrand;

   // Make Sure it's ASCII or UTF-8
   $str_encoding = mb_detect_encoding($content);

   if ( $str_encoding != 'ASCII' && $str_encoding != 'UTF-8' ) {
        return array (0, 'Content encoding must be ASCII or UTF-8');
   }

   //elog("Str Encoding : $str_encoding ... ");

   return array (1, 'OK');

}


// 'input_send' (input by email) needs to call this massage 2 times, otherwise only 1 time
// $ehash['target_selection'], 'S' is to subscribed users only, 'UN' is to unsbuscribed & not subscribed users
// $ehash['allow_duplicate_send'], default is '0'

function massage_edm_users ($mode, $ehash, &$users, &$filter) {

   global $cobrand;

   // if not $ehash['allow_duplicate_send'] passed in, initalize to '0'
   $ehash['allow_duplicate_send'] = isset($ehash['allow_duplicate_send']) ? $ehash['allow_duplicate_send'] : 0;

   // if use member id, then must allow send duplicate
   list ($rc, $rm, $ehash['use_member_id']) = get_option ('edm', 'GL.useMemberId');
   if ( $ehash['use_member_id'] ) { $ehash['allow_duplicate_send'] = 1; }

   // elog("mode : $mode");
   // elog("ehash : " .  print_r($ehash, true));
   // elog(print_r($filter, true));
   // elog(" ... edm hash merge ... ");
   // elog(print_r($ehash['edm_merge'], true));
   // elog("users array : \n" . print_r($users, true));
   // exit;


   $start_memory = memory_get_usage (true);

   elog("Massage Start -> Start Memory : $start_memory");

   /************************ if import , check header, if fail, return *******************************/

   if ( $mode == 'import' ) {

        // pass first row ($users[0]) to check_header_format to validate
        // $ehash['header'] will be used in get_xl_users
        list ($rc, $rm, $ehash['header'], $header_result) = check_header_format ('edm', $ehash, $users[0]);

        // when frontend pass '0' (off the mode) that means it's been checked before
        if ( !$rc && $ehash['report_header_error'] ) {   

             return array (0, 'Header Error', $header_result, 0, 0, 0, 0, 0, 
                    array(), array(), array(), array(), array(), array(), array(), 0, 0, array(), 0);

        }  

        $ehash['num_of_xls_rows'] = count($users);
        elog ("no. of rows : " . $ehash['num_of_xls_rows']);

   }


   /*************************** initialize variables ********************************/

   $stat_figures['num_imported_users'] = 0;
   $stat_figures['num_input_users'] = 0;
   $stat_figures['num_users_sent'] = 0;      // this no. will exclude invalid format and duplicate emails
   $stat_figures['num_exclude_send'] = 0;    // if required, this will report no. of previously sent
   $stat_figures['num_filtered'] = 0;       // if required, this will report no. of filtered those not matching user response action


   $return_lists['incorrect_email_list'] = array();
   $return_lists['duplicate_email_list'] = array();
   $return_lists['not_subscribed_email_list'] = array();
   $return_lists['unsend_email_list'] = array();
   $return_lists['original_not_subscribed_list'] = array();
   $return_lists['original_unsend_list'] = array();
   //$return_lists['incorrect_import_lang_list'] = array();
   $return_lists['sendout_email_list'] = array();
   $return_lists['no_mockup_right_email_list'] = array();

   $sendout_member_id_list = array();

   // elog ("Massage Available Lang : " . $ehash['available_lang']);

   // available_lang comes in as 'none' OR comma delimited if it has multiple languages
   $ehash['available_lang'] = trim($ehash['available_lang']); 

   if ( empty($ehash['available_lang']) ) {
      	$ehash['available_lang'][0] = 'none';
   } else {
       	$ehash['available_lang'] = explode(',', $ehash['available_lang']);   // force $ehash['available_lang'] to become an array
   }

   elog ("Massage Available Lang Array : " . print_r($ehash['available_lang'], true));

   $ca = count($users);
   //elog ("Come In Users : $ca");
 

   // mockup needs also get mockup whitelist
   if ( $mode == 'mockup' ) {
        list ($rc, $rm, $ehash['mockup_allowed_emails']) = get_option ('edm', 'EDM.mockupAllowedEmails');
        $ehash['mockup_allowed_emails'] = trim($ehash['mockup_allowed_emails']);
   }



   $mtime = microtime(); 
   $mtime = explode(" ",$mtime); 
   $mtime = $mtime[1] + $mtime[0]; 
   $starttime = $mtime; 


   /***************************************** start loop users ******************************************/

   $cobrand['solr_ch'] = curl_init();
   $row = 0;

   foreach ( $users as $key => $entry ) {

      	// initialize this user variables
      	// $row = $row + 1;

        // initialize this user variables
        $user = array();
        $user['row'] = ++$row;

        if ( $mode == 'import' ) {    

             list ($rc, $rm) = get_xls_user ('edm', $ehash, $user, $entry);
             if ( !$rc )  { continue; }
             ++$stat_figures['num_imported_users'];   // Excluded Empty Rows, Count one before continue

        } else {      // $mode = 'mockup' or 'send_list_send' or 'input_send'

             $user = array_merge($user, $entry);
             if ( $mode == 'input_send' ) {  ++$stat_figures['num_input_users'];  }

        }

        // elog (print_r($user, true));
        $user['email'] = trim($user['email']);
      	$user['email'] = str_replace(chr(0xC2) . chr(0xA0), '', $user['email']);  // Trim ALT-0160
        $user['email'] = strval($user['email']);
        $user['email'] = strtolower($user['email']);   
        $user['sc_email'] = '';
        $user['prefixed_email'] = '';

      	// work out this naked_email making sure it's no member_id prefixed for validation

      	if ( $ehash['allow_duplicate_send'] ) {  // if it's by import it has to concat and check
      			 
             list ($rc, $rm) = _make_email_prefix ($mode, $ehash, $user);

       	} else {  // email as unique

             $user['naked_email'] = $user['email'];  

      	}

        /***************************** Make Unique Key *******************************/
        if ( $ehash['allow_duplicate_send'] ) {

             if ( !empty($user['prefix']) ) {
                  $user['prefixed_email'] = $user['prefix'] . '||' . $user['naked_email'];
                  $user['unique_key'] = $user['prefix'];
             } else {
                  $user['prefixed_email'] = $user['naked_email'];
                  $user['unique_key'] = $user['prefixed_email'];
             }

        } else {

             $user['prefixed_email'] = $user['naked_email'];

      	}

        //$user['prefixed_email'] = $user['email'];   // no matter it has || or not, it's prefixed to store in edm_stats
        $user['sc_email'] = strtolower($user['prefixed_email']);   // small case for checking unsend list
        $user['email'] = $user['naked_email']; 

        // elog ("user : " . print_r($user, true));
        // elog ("mode : $mode");
      	//elog (" Row : $row , Email : " . $user['email'] . ", SC Email : " . $user['sc_email']);
      	//elog (" Prefixed Email : " . $user['prefixed_email'] . " , Naked Email : " . $user['naked_email']);
      	//elog (" No. Imported Users : " . $stat_figures['num_imported_users'] . ", No Input Users : " . $stat_figures['num_input_users']);


      	/**************** Check Email Format (And Member ID, if any) *********************/
      	// Later add mode 'send_list' can skip check incorrect email format
        // Check Invalid Email Format (frontend reported as incorrect emails)

        // $format_error = 0;



      	if ( $mode != 'send_list_send' ) {   // Send List Send can skip check member id and email format

             list ($rc, $rm) = _check_incorrect_email ($ehash, $user, $return_lists);
             if ( !$rc )  { continue; }   // stop this user


      	     /********************** Check if email is in allowed mockup list ***************************/ 
      	     // restrict mockup emails only send to what domains
       	     // support exact email and wildcard, delimited by comma (e.g. hello@gmail.com,*@ufosend.com)

             if ( $mode == 'mockup' ) {

                  list ($rc, $rm) = _check_mockup_allowed_email ($ehash, $user, $return_lists);
                  if ( !$rc )  { continue; }   // stop this user

             }

             /********************** Check if email is duplicated ***************************/ 
             // note, if allows duplicate, this function will handle that also
             list ($rc, $rm) = _check_duplicate_email ($ehash, $user, $return_lists);
             if ( !$rc )  { continue; }   // stop this user

             // send to subscribed uses only, it needs filter those not subscribed
             if ( $ehash['target_selection'] == 'S' ) {
                  
                  list ($rc, $rm) = _check_not_subscribed_email ($ehash, $user, $return_lists);
                  if ( !$rc )  { continue; }   // stop this user

             }

             // if it's send list, it's been excluded by frontend side already
             // so, here only caters those not send by send list
             if ( $ehash['exclude_previously_sent'] ) {
                  
                  list ($rc, $rm) = _check_previously_sent_email ($ehash, $user, $stat_figures);
                  if ( !$rc )  { continue; }   // stop this user

             }

             list ($rc, $rm) = _check_unsend_list_email ($ehash, $user, $return_lists);
             if ( !$rc )  { continue; }   // stop this user
      

             if ( $mode != 'mockup' ) {    // if send list send, this part done in cake

                  $need_exclude = 0;
                  $need_exclude = _do_solr_filter ($filter, $user['email'], $stat_figures);
                  // elog("need exclude : $need_exclude");
                  if ( $need_exclude )  { continue; }   // that means, this user does not match criteria, then no need send to him

             }


        }
        


      	/******************* Check if wrong send_lang (apply to import only) ************************/

        list ($rc, $rm) = get_send_lang ('edm', $ehash, $user);

      	// $user['send_lang'] = '';



      	// /********************* convert prefer_lang to send_lang for system use ************************/

       //  if ( isset($user['prefer_lang']) ) {

       //  	   $user['prefer_lang'] = strtolower($user['prefer_lang']);   

       //       // Do mapping
       //       if ( $user['prefer_lang'] == 'en' ) { $user['send_lang'] = 'eng';   }
       //       if ( $user['prefer_lang'] == 'tc' ) { $user['send_lang'] = 'zh_hk'; }
       //       if ( $user['prefer_lang'] == 'sc' ) { $user['send_lang'] = 'zh_cn'; }

      	// } else {

      	//      //elog("reg lang : " . $ehash['reg_lang']);
      	//      $user['send_lang'] = isset($user['reg_lang']) ? $user['reg_lang'] : '';
      	//      unset($user['reg_lang']);
      	//      // elog("send lang before : " . $user['send_lang']);

      	// }

      	// ******************** convert prefer_lang to send_lang for system use ***********************
       //  // available_lang refers to what lang. defined for that campaign
       //  // if chose no need multiple lang, then available_lang is 'none'

      	// // User's lang not in available lang
      	// if ( $ehash['available_lang'][0] == 'none' ) { 

      	//      $user['send_lang'] = '';

      	// } else { 

       //       // if send lang is not in available lang, force to use first lang
       //       if ( !in_array($user['send_lang'], $ehash['available_lang']) ) { 

      	//        	  $user['send_lang'] = $ehash['available_lang'][0];

      	//      }

      	// }


      	// // Force to use that selected lang anyway
       //  if ( $ehash['selected_lang'] != 'smart' ) {    
      	//      $user['send_lang'] = $ehash['selected_lang'];
       // 	}


	      // elog("send lang after : " . $user['send_lang']);


        /******************************** Check if fits filter criteria *************************************/
        // do this before check not subscribe 

       //  if ( $mode == 'import' || $mode == 'input_send' ) {    // if not import, this part done in cake

    	  //     $need_exclude = 0;
    	  //     $need_exclude = _do_solr_filter ($filter, $user['email'], $stat_figures);
    	  //     // elog("need exclude : $need_exclude");
    	  //     if ( $need_exclude )  { continue; }   // that means, this user does not match criteria, then no need send to him

	      // }

        // elog("here 5.. ");

         /******************************** Check Not Subscribed Emails *************************************/

        //  $hit_not_subscribed_list = 0;

      	 // // target_selection = S means send to Subscribed users only
      	 // // Send List Send can skip this check
        //  if ( $mode != 'send_list_send' && $ehash['target_selection'] == 'S' ) {    
               
        //       list ($rc, $rm) = _check_not_subscribed_email ($ehash, $user, $return_lists);
        //       if ( !$rc )  { continue; }   // stop this user

        //  }          


         /**************** Not Send to All (i.e. Exclude Those Previously Sent) ***********************/ 

         // if it's send list, it's been excluded by frontend side already
         // if ( $ehash['exclude_previously_sent'] && $mode != 'send_list_send' )  {    

         //      list ($rc, $rm) = _check_previously_sent_email ($ehash, $user, $stat_figures);
         //      if ( !$rc )  { continue; }   // stop this user

         // }


      	/******************************** Check Unsend List Emails *************************************/

       //  if ( $mode != 'send_list_send' ) {    // Send List Send can skip this check

       //       list ($rc, $rm) = _check_unsend_list_email ($ehash, $user, $return_lists);
       //       if ( !$rc )  { continue; }   // stop this user

	      // }


      	/********************* Put Qualified Emails to $sendout_email_list ***************************/



        // elog ("before call availab_lang here : " . print_r($ehash['available_lang'], true));
        foreach ( $ehash['available_lang'] as $tmp_lang ) {

            // elog (" >>>>> tmp lang : " . $tmp_lang);
            // elog (" >>>>> send lang : " . $user['send_lang']);

            // allow tmp_lang = 'none' to go  ... that case, even send_lang = '', detect name will cater
            if ( ($user['send_lang'] != $tmp_lang) && ($tmp_lang != 'none') ) {
                 continue;
            }

      		  // elog ("tmp lang in for loop : $tmp_lang");
      		  // elog ("send massage : " . $ehash['send_massage']);

      		  // if it's not a send massage, that is calculate unique users only, no need merge
            if ( $ehash['edm_merge'][$tmp_lang] && $ehash['send_massage'] ) {   

  	             // If no need merge, don't need to care prefixed mobile
  	             if ( isset($user['mobile']) ) {

                	    if ( strpos($user['mobile'], '||') !== false ) {
                           list ($dummy_member_id, $user['mobile']) = explode("||", $user['mobile']);
                     	}    

                     	if ( strpos($user['mobile'], '.') !== false ) {
                           list ($user['country_code'], $user['mobile']) = explode(".", $user['mobile']);
      		   	        }

                  }   

                  // elog ("\n\n\n");
                  // elog (print_r($ehash, true));
                  // elog (" >>>>> tmp lang : " . $tmp_lang);
                  // elog (" >>>>> firstname : " . $user['firstname']);
                  // elog (" >>>>> lastname : " . $user['lastname']);
                  // elog (" >>>>> fullname : " . $user['fullname']);
                  // elog (" >>>>> salutation : " . $user['salutation']);
                  // elog (" >>>>> send lang  : " . $user['send_lang']);
                  // elog (" >>>>> content : " . $ehash['edm_content'][$tmp_lang]);

                  // edmdesigner converted $ to &#36;
                  if ( preg_match("/\\\$system_detect_name\\\$/si" , $ehash['edm_content'][$tmp_lang]) || 
                       preg_match("/%24system_detect_name%24/si" , $ehash['edm_content'][$tmp_lang]) ||
                       preg_match("/&#36;system_detect_name&#36;/si" , $ehash['edm_content'][$tmp_lang]) ) {

                       $user['system_detect_name'] = detectName('edm', $user);

      		        }

                  list ($rc, $rm) = makeup_user_data ('edm', $user);
                  // elog ("\n\n");
                  // elog(print_r($user, true));
                  
                  // elog (" >>>>> system_detect_name : " . $user['system_detect_name']);
                  // elog (" >>>>> salutation 2 : " . $user['salutation']);
                  // // exit;
      		        
                  // Get Param of This User (for campaign page's merge user)
                  list ($new_edm_content, $params_hash) = merge_content ('edm', 'normal', $user, $ehash['edm_content'][$tmp_lang]);
                  $user['params_hash'] = $params_hash;
                  // elog ("param hash : " . print_r($user['params_hash'], true));
                  // elog ("Here after edm merge : $new_edm_content ... ");

            } 

        }


        /******************************** Add to Send Out List *************************************/

      	 if ( $ehash['allow_duplicate_send'] ) {

              $sendout_member_id_list[$user['unique_key']] = $user['row'];

      	 }


         $return_lists['sendout_email_list'][$user['prefixed_email']] = $user;       // Must Be Unique, So Use 'email' as Key

         ++$stat_figures['num_users_sent'];

   }

   curl_close ($cobrand['solr_ch']);

   $users = null;   // Release Memory

   /***************************************** end loop users ******************************************/


   // mysqli_autocommit($cobrand['link'], TRUE);

   $mtime = microtime(); 
   $mtime = explode(" ",$mtime); 
   $mtime = $mtime[1] + $mtime[0]; 
   $endtime = $mtime; 
   $totaltime = ($endtime - $starttime); 
   $end_memory = memory_get_usage (true);
   $memory_use = $end_memory - $start_memory;

   $peak_memory_use = memory_get_peak_usage (true);
             
   elog("\n\n------------------------------ Count ---------------------------------\n");
   elog("No. of imported users : " . $stat_figures['num_imported_users'] );
   elog("\n");
   elog("Num Users Sent : " . $stat_figures['num_users_sent'] );
   elog("Num Incorrect Format Users : " . count($return_lists['incorrect_email_list']) );
   elog("Num Duplicate Users : " . count($return_lists['duplicate_email_list']) );
   elog("Num In Unsend List Users : " . count($return_lists['unsend_email_list']) );
   elog("\n");
   elog("This page was created in ".$totaltime." seconds"); 
   elog("Massage Start -> Start Memory : $start_memory");
   elog("Massage End -> End Memory : $end_memory");
   elog("Memory Use : $memory_use");
   elog("Peak Memory Use : $peak_memory_use");

   //exit;

   if ( $stat_figures['num_users_sent'] == 0 ) {
        $rm = 'No valid users on list OR all users have unsubscribed newsletters!';
   } else {
        $rm = 'OK';
   }


   // elog ("NOTE : end of massage : return list :\n" . print_r($return_lists, true) . "\n\n");

   /******************************* Return ***********************************/
   return array (1, $rm, array(), $stat_figures, $return_lists);

}


function _make_email_prefix ($mode, &$ehash, &$user) {

     global $cobrand;

     // if it's by input_send, the email field already is prefixed email    
     if ( $ehash['use_member_id'] ) {
          $user['prefix'] = isset($user['member_id']) ? $user['member_id'] : '';
     } else {
          $user['prefix'] = $user['row'];  // use row to make unique key
     }  

     // elog ("Prefix : " . $user['prefix']);

     if ( $mode == 'import' ) {

          $user['prefix'] = trim($user['prefix']);
          $user['prefix'] = str_replace(chr(0xC2) . chr(0xA0), '', $user['prefix']);
          $user['prefix'] = strtoupper($user['prefix']);
          $user['naked_email'] = $user['email'];
          //$user['unique_key'] = $user['member_id'];
          //$user['email'] = $user['member_id'] . '||' . $user['email'];

          if ( isset($user['mobile']) ) {
               $user['naked_mobile'] = $user['mobile'];
               $user['mobile'] = $user['prefix'] . '||' . $user['mobile'];
          }

     } else {   // maybe by send by input

          if ( strpos($user['email'], '||') !== false ) {

               list ($tmp_prefix, $tmp_email) = explode("||", $user['email'], 2);
               $user['prefix'] = strtoupper($tmp_prefix);   
               $user['naked_email'] = $tmp_email;

          } else {

               $user['naked_email'] = $user['email'];  
               //$user['unique_key'] = $user['email'];   // fake the system, to avoid duplicate member id
               //elog (" here has no ||");

          }

     }

     return array (1, 'OK');

}


// call by massage
function _check_incorrect_email (&$ehash, &$user, &$return_lists) {

     global $cobrand;

     /******************** Check Incorrect Member ID format, if any ************************/
     if ( $ehash['use_member_id'] ) { 

           if ( !empty($user['prefix']) && !preg_match("/^[_A-Z0-9-]+$/", $user['prefix']) ) {

                 elog ("     >> Incorrect Member ID : " . $user['prefix']);
                 $user['desc_code'] = 'IMIF';
                 array_push ($return_lists['incorrect_email_list'], $user);  
                 return array (0, 'Incorrect Email Format');

           }
                     
      }         
                     
      /*********************** Check Incorrect Email Format ******************************/ 

      // check email format
      if ( !preg_match("/^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[_a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$/", $user['naked_email']) ) {

           $user['desc_code'] = 'IEF';  // Incorrect Email Format
           array_push ($return_lists['incorrect_email_list'], $user);  
           return array (0, 'Incorrect Email Format');

      } else if ( preg_match("/@sina([_a-z0-9-]+).com$/", $user['naked_email']) || 
                  preg_match("/@mysinamail.com$/", $user['naked_email']) ||
                  preg_match("/@ovi.com$/", $user['naked_email']) ) {

           $user['desc_code'] = 'DDE';  // Discontinued Domain Email
           array_push ($return_lists['incorrect_email_list'], $user);  
           return array (0, 'Incorrect Email Format');

      }

      return array (1, 'OK');

}


// call be massage
// for bank compliance, needs to add whitelist for mockup
// i.e. avoid send mockup to other users that outside of bank
function _check_mockup_allowed_email (&$ehash, &$user, &$return_lists) {

      global $cobrand;

      if ( !empty($ehash['mockup_allowed_emails']) ) {

           $mockup_allowed_email_array  = explode (',', $ehash['mockup_allowed_emails']);
           $hit_allowed_email_count = 0;    // count if it hits the allowed list, if none is hit, then disallow

           foreach ( $mockup_allowed_email_array as $allowed_email ) {

                  $allowed_email = preg_replace ('/^\*@/', '@', trim($allowed_email));
                  // elog ("allowed email : $allowed_email");

                  if ( preg_match("/$allowed_email$/", $user['naked_email']) ) {
                     $hit_allowed_email_count++; 
                     //echo ">> Hit ... <BR><BR>";
                  }            

           }                 

           // not defined in allowed list
           if ( $hit_allowed_email_count == 0 ) { 
                $user['desc_code'] = 'NAR';   // Email not allowed to receive mockup
                array_push ($return_lists['no_mockup_right_email_list'], $user);       // No Need Unique, for Reporting
                elog ("    >>> Email not allowed to receive mockup : " . $user['email']);
                return array (0, 'Email not in mockup whitelist');
           }

      }  

      return array (1, 'OK');

}


// call be massage
// $ehash['passed_list'] is to hold those unquie keys not duplicated
function _check_duplicate_email (&$ehash, &$user, &$return_lists) {

    global $cobrand;

    // elog (print_r($user,true));

    if ( $ehash['allow_duplicate_send'] )  {  $pass_key = $user['unique_key'];         }
    else                                   {  $pass_key = $user['prefixed_email'];     }

    //elog("pass key : $pass_key");

    if ( isset($ehash['passed_list'][$pass_key]) ) {

         $user['duplicate_with_row'] = $ehash['passed_list'][$pass_key];
         array_push ($return_lists['duplicate_email_list'], $user);     // No Need Unique, for Reporting
         return array (0, 'Duplicate Email');

    }

    $ehash['passed_list'][$pass_key] = $user['row'];

    return array (1, 'OK');

}

// call be massage
function _check_not_subscribed_email (&$ehash, &$user, &$return_lists) {

    global $cobrand;

    // elog ("  Email >>> : " . $user['sc_email']);

    $phash = null;
    $phash['condition'] = array();

    $phash['remark'] = 'Check if this user (collection_users) has not subscribed edm';
    $phash['collection'] = 'users';
    $phash['fields'] = 'email, naked_email';

    array_push($phash['condition'], ' AND (email:"' . $user['sc_email'] . '")');

    // newsletter = -1 means not activated, -999 means not subscribed
    array_push($phash['condition'], ' AND (newsletter:"-1" OR newsletter:"-999")');   

    // use users list curl handler
    list ($rc, $rm, $num_records, $solr_hash) = connect_solr ('edm', $cobrand['solr_ch'], 'select', $phash);

    // elog ("  before hit record : $num_records \n\n");
    if ( $num_records == 1 )  {   // that means, this user has not subscribed edm

         // elog ("  hit record \n\n");
         $hit_not_subscribed_list = 1;

         $user['desc_code'] = 'ENS';
         array_push ($return_lists['not_subscribed_email_list'], $user);       // No Need Unique, for Reporting
         return array (0, 'This email not subscrbied to receive eDM');

    }

    return array (1, 'OK');

}


// call be massage
function _check_previously_sent_email (&$ehash, &$user, &$stat_figures) {

    global $cobrand;

    $previously_sent_campaigns = array();

    $phash = null;
    $phash['condition'] = array();
    $tmp_stmt = '';

    $phash['remark'] = 'Check if this user we have sent this campaign before';
    $phash['collection'] = 'edm_stats';
    $phash['solr_limit'] = '0';

    if ( !empty($ehash['copy_from']) ) {
         $previously_sent_campaigns = explode (',', $ehash['copy_from']);
    }

    array_push ($previously_sent_campaigns, $ehash['campaign_id']);

    foreach ( $previously_sent_campaigns as $cid ) {
              $tmp_stmt .= ' OR (campaign_id:"' . $cid . '")';
    }

    $tmp_stmt = preg_replace ('/^ OR /', '', $tmp_stmt);
    array_push($phash['condition'], ' AND (' . $tmp_stmt . ')');
    array_push($phash['condition'], ' AND (email:"' . $user['sc_email'] . '")');
    array_push($phash['condition'], ' AND (release_sequence:{0 TO *})');   // don't count mockup

    list ($rc, $rm, $num_records, $solr_hash) = connect_solr ('edm', $cobrand['solr_ch'], 'select', $phash);
    // elog ("tmp stmt : $tmp_stmt");
    // elog ("no. of matching sent before : $num_records");
    if ( $num_records > 0 ) { 
         ++$stat_figures['num_exclude_send'];
         return array (0, 'Has previously sent to this email before');
    }     // that means, has sent before 

    return array (1, 'OK');

}


// call be massage
function _check_unsend_list_email (&$ehash, &$user, &$return_lists) {

    global $cobrand;

    // elog ("  Email >>> : " . $user['sc_email']);

    $phash = null;
    $phash['condition'] = array();

    $phash['remark'] = 'Check if this user (collection_edm_unsend_lists) is in unsend list';
    $phash['collection'] = 'edm_unsend_lists';
    $phash['fields'] = 'email, naked_email, campaign_id, release_sequence, webmaster_action,
                        status, ab_status, desc_code, description, created';

    array_push($phash['condition'], ' AND (email:"' . $user['sc_email'] . '")');

    if ( $ehash['target_selection'] == 'UN' ) {
         array_push($phash['condition'], ' AND NOT (status:"UU")');
    }

    // use unsend list curl handler
    list ($rc, $rm, $num_records, $solr_hash) = connect_solr ('edm', $cobrand['solr_ch'], 'select', $phash);

    //$count = count($solr_hash);
    // elog("count : $count, num records : $num_records");

    if ( $num_records == 1 )  {  // that means, it is in unsend list

         $ul_user = $solr_hash[0];

         if ( $ul_user['status'] == 'AB' ) {

              $user['desc_code'] = $ul_user['status'] . ':' . $ul_user['desc_code'];   // AB:abuse / AB:hbounce / AB:3TMF
              $user['ab_status'] = $ul_user['ab_status'];

         } else {

              if ( $ul_user['status'] == 'WA' && $ul_user['desc_code'] == '3TTU' ) {
                   $user['desc_code'] = $ul_user['desc_code'];   // 3TTU
              } else {
                   $user['desc_code'] = $ul_user['status'];    // WA / UU
              }

         }


         $user['description'] = $ul_user['description'];

         // compose previous_failed_in value
         $tmp_cid = !empty($ul_user['campaign_id']) ? $ul_user['campaign_id'] : 'NA';
         $tmp_rs = !empty($ul_user['release_sequence']) ? $ul_user['release_sequence'] : 'NA';

         $user['previous_failed_in'] = $tmp_cid . '||' . $tmp_rs . '||' . $ul_user['webmaster_action'] . '||' . $ul_user['created'];

         array_push ($return_lists['unsend_email_list'], $user);       // No Need Unique, for Reporting
         return array (0, 'This email is in unsend list');

         //elog ("    >>> Unsend List Email Addr. : " . $user['email']);
         //elog ("        >>> Desc Code : " . $user['desc_code']);
         //elog ("        >>> Desc : " . $user['description']);
         // continue;

    }

    return array (1, 'OK');

}


function _edm_finalize (&$send_hash, &$stat_figures, $num_files) {

     global $cobrand;

     /************ Re-initalize Param (for readiness convenience only) ****************/

     $service_plan = $send_hash['service_plan'];
     $campaign_id = $send_hash['campaign_id'];
     $batch_id = $send_hash['batch_id'];
     $post_datetime = $send_hash['post_datetime'];
     $mockup = $send_hash['mockup'];
     $schedule_send = $send_hash['schedule_send'];
     $schedule_send_id = isset($send_hash['schedule_send_id']) ? $send_hash['schedule_send_id'] : '';
     $seq_field = $send_hash['seq_field'];
     $seq_num = $send_hash['seq_num'];

     // if call by api, we don't know whether it's superuser login, so it must be 0
     $superuser_login = isset($send_hash['superuser_login']) ? $send_hash['superuser_login'] : 0;

     // $schedule_send_id = isset($send_hash['schedule_send_id']) ? $send_hash['schedule_send_id'] : '';

     $num_users_sent = $stat_figures['num_users_sent'];
     $num_exclude_send = $stat_figures['num_exclude_send'];

     // elog ("send hash : " . print_r($send_hash, true));

     /**************************** Initalize Other Param *****************************/

     $year = substr($post_datetime, 0, 4);
     $month = substr($post_datetime, 4, 2);
     $day = substr($post_datetime, 6, 2);

     $delivery_date = $year . '-' . $month . '-' . $day;
     $sbatch_hash = array();

     list ($current_datetime, $current_year, $current_month, $current_day, $current_hour) = get_current_datetime();


     /************************* Update Batch Record *****************************/
     // Get Blocked Count

     $data['num_users_sent'] = $num_users_sent;
     $data['mockup'] = $mockup;

     $additional_users = 0;
     $refund_users = 0;

     if ( $schedule_send ) {

          // Scheduled Batch Info
          $sbatch_hash = get_batch_info ('edm', $batch_id);
          $batch_scheduled_users = $sbatch_hash['num_scheduled_users'];

          $additional_users = $num_users_sent - $batch_scheduled_users;
          if ( $additional_users < 0 ) { $additional_users = 0; }

          $refund_users = $batch_scheduled_users - $num_users_sent;
          if ( $refund_users < 0 ) { $refund_users = 0; }

          $data['additional_users'] = $additional_users;
          $data['refund_users'] = $refund_users;

          // Write Figures to Schedule Send  (no need to run this, 23 Apr 2014)
          // list ($rc, $rm) = update_schedule_send ('edm', 'after_sendout', $schedule_send_id, '', $num_users_sent, '', '');

     }

     /********** Special arrangement for auto responder, never count mockup **************/

     if ( $campaign_id < 0 ) {

          // also, if 1st time we allow to write post datetime, afterwards don't need
          // this is because for statistics purpose

          if ( $seq_num == 1 ) {
               $post_datetime_str = " post_datetime = '$post_datetime', ";
          } else {
               $post_datetime_str = "";
          }

     }

     /******************************** Update batches ***********************************/

     if ( $campaign_id < 0 ) {   // Add figures, instead of write figures

          if ( $mockup ) {

                $update_batch_sql = "UPDATE batches 
                                        SET $seq_field = '$seq_num',
                                            $post_datetime_str
                                            modified = '$current_datetime',
                                            superuser_login = '$superuser_login' 
                                      WHERE campaign_id = '$campaign_id'
                                        AND id = '$batch_id'";

          } else {

                //$post_datetime_str = " post_datetime = '$post_datetime', ";

                $update_batch_sql = "UPDATE batches 
                                        SET $seq_field = '$seq_num',
                                            $post_datetime_str
                                            num_files = '$num_files',
                                            num_effective_users = num_effective_users + $num_users_sent,
                                            num_users_sent = num_users_sent + $num_users_sent,
                                            num_excluded = num_excluded + $num_exclude_send,
                                            modified = '$current_datetime',
                                            superuser_login = '$superuser_login' 
                                      WHERE campaign_id = '$campaign_id'
                                        AND id = '$batch_id'";

          }

      } else {

          $update_batch_sql = "UPDATE batches 
                                  SET $seq_field = '$seq_num',
                                      post_datetime = '$post_datetime',
                                      num_files = '$num_files',
                                      num_scheduled_users = 0,
                                      num_effective_users = num_effective_users + $additional_users - $refund_users,
                                      num_users_sent = '$num_users_sent',
                                      num_excluded = '$num_exclude_send',
                                      modified = '$current_datetime',
                                      superuser_login = '$superuser_login' 
                                WHERE campaign_id = '$campaign_id'
                                  AND id = '$batch_id'";

     }

     elog ("Update Batches SQL (_finalize) : $update_batch_sql");
     mysqli_query($cobrand['link'], $update_batch_sql);

     if ( !$mockup ) {

           // Update campaign last sent datetime
           $update_campaign_last_sent_sql = "UPDATE campaigns 
                                                SET last_sent_datetime = '$current_datetime' 
                                              WHERE id = '$campaign_id'";
          
           elog ("Update Last Sent Datetime SQL : $update_campaign_last_sent_sql");
           mysqli_query($cobrand['link'], $update_campaign_last_sent_sql);

     }

     /************************* Update Monthly Report Record *****************************/

     list ($rc, $rm) = _edm_update_monthly_report ('after_sendout', $schedule_send, $sbatch_hash, $batch_id, $mockup, $data, $delivery_date, $year, $month);

     if ( $service_plan == 'TRI' || $service_plan == 'AVS' ) {
          list ($rc, $rm) = _edm_update_monthly_report ('after_sendout', $schedule_send, $sbatch_hash, $batch_id, $mockup, $data, 'dummy', '0', '0');
     } else {
          list ($rc, $rm) = check_and_update_exceeded_quota ($delivery_date);   // Trial and AVS no need update this
     }

     return array (1, 'OK', $seq_num);

}



function _write_to_batches ($schedule_send, $schedule_send_id, $campaign_id, $batch_id, 
			    $num_imported_users, $num_input_users, $num_users_sent, $data, $mockup, $debug) {

      global $cobrand;

      $num_scheduled_users = 0;
      list ($current_datetime, $current_year, $current_month, $current_day, $current_hour) = get_current_datetime();

      $seq_num = 0;

      // Write to DB
      if ( $schedule_send == 1 ) {

	         $num_scheduled_users = $num_users_sent;

      	   if ( !empty($data['schedule_sequence']) ) {
      		      $seq_num = $data['schedule_sequence'];
      	   } else {
           	   	$get_sequence_sql = "SELECT MAX(schedule_sequence) AS seq_num FROM batches WHERE campaign_id = '$campaign_id'";
               	$get_sequence_result = mysqli_query($cobrand['link'], $get_sequence_sql);
               	$seq_row = mysqli_fetch_assoc($get_sequence_result);
           	   	$seq_num = $seq_row['seq_num'] + 1;
      	   }

           // Write Figures to Schedule Send
           list ($rc, $rm) = update_schedule_send ('edm', 'before_sendout', $schedule_send_id, $seq_num, $num_users_sent, '', '');

      }

      // num of imported users has been written to batch during creation of batch id
      // so, here does not need to add again, we only need to add to monthly reports
      $update_batch_sql = "UPDATE batches SET campaign_id = '$campaign_id',
                                        	    mockup = '$mockup',
                                         	    debug = '$debug',
                                              schedule_sequence = '$seq_num',
                                    	        num_imported_users = '$num_imported_users',
                                      	      num_scheduled_users = '$num_scheduled_users',
                                              num_effective_users = '$num_users_sent',
              		                            modified = '$current_datetime'
                                        WHERE id = '$batch_id'";
		
      $result = mysqli_query($cobrand['link'], $update_batch_sql);

      elog ("update batch sql (write to batches) : \n$update_batch_sql");
      //echo "batch id : $batch_id<BR>";

      return array (1, 'OK');

}


function _write_to_edm_stats (&$send_hash, &$return_lists) {

      global $cobrand;

      /***************** Re-initalize Param (for readiness convenience only) ***************************/
      $campaign_id = $send_hash['campaign_id'];
      $batch_id = $send_hash['batch_id'];
      $seq_field = $send_hash['seq_field'];
      $seq_num = $send_hash['seq_num'];
      $is_trial = $send_hash['is_trial'];


      mysqli_autocommit($cobrand['link'], FALSE);

      $delivery_datetime = date('Y-m-d H:i:s', mktime(date("H"), date("i"), date("s"), date("m"), date("d"),  date("Y")));

      if ( $seq_field == 'mockup_sequence' ) {  $seq_num = 0;  }

      // Unsend List -- Flag From unsend list status
      if ( count($return_lists['unsend_email_list']) > 0 ) {

           foreach ($return_lists['unsend_email_list'] as $key => $user ) {

                 $user['description'] = addslashes($user['description']);

                 // elog ("description : " . $user['description']);

                 $user_id = isset($user['id']) ? $user['id'] : '';
            		 $email = $user['prefixed_email'];
            		 $desc_code = $user['desc_code'];
            		 $description = $user['description'];
            		 $previous_failed_in = $user['previous_failed_in'];
            		 //$lang = $user['send_lang'];
                 $lang = isset($user['send_lang']) ? $user['send_lang'] : '';   // sometimes api comes in duplicate has not this lang value


                 $insert_edm_stats_sql = "INSERT INTO edm_stats (`campaign_id`, 
                                                                 `batch_id`, 
                                                                 `lang`, 
                                                                 `release_sequence`,
                                                								 `user_id`, 
                                                                 `email`, 
                                                                 `delivery_status`,
                                                								 `delivery_failure_code`, 
                                                                 `desc_code`, 
                                                                 `previous_failed_in`,
                                                								 `description`, 
                                                                 `is_trial`, 
                                                                 `created`, 
                                                                 `modified`)
                                                         VALUES ('$campaign_id', 
                                                                 '$batch_id', 
                                                                 '$lang', 
                                                                 '$seq_num', 
                                                								 '$user_id', 
                                                                 '$email', 
                                                                 'BK', 
                                                								 'UL', 
                                                                 '$desc_code', 
                                                                 '$previous_failed_in', 
                                                                 '$description', 
                                                                 '$is_trial', 
                                                                 '$delivery_datetime', 
                                                                 '$delivery_datetime')";

                 //elog ("Insert SQL : $insert_edm_stats_sql");
                 $result = mysqli_query( $cobrand['link'] , $insert_edm_stats_sql );

           }

      }


      // NS - Not Subscribed List 
      if ( count($return_lists['not_subscribed_email_list']) > 0 ) {

           foreach ($return_lists['not_subscribed_email_list'] as $key => $user ) {

            		 $user_id = isset($user['id']) ? $user['id'] : '';
                 $email = $user['prefixed_email'];
    	           //$lang = $user['send_lang'];
                 $lang = isset($user['send_lang']) ? $user['send_lang'] : '';   // sometimes api comes in duplicate has not this lang value

                 $insert_not_subscribed_email_sql = "INSERT INTO edm_stats (`campaign_id`, 
                                                                            `batch_id`, 
                                                                            `lang`, 
                                                                            `release_sequence`,
                                                      									    `user_id`, 
                                                                            `email`, 
                                                      									    `delivery_status`, 
                                                                            `delivery_failure_code`, 
                                                                            `desc_code`,
                                                       									    `description`, 
                                                                            `is_trial`, 
                                                                            `created`, 
                                                                            `modified`)
                                                                    VALUES ('$campaign_id', 
                                                                            '$batch_id', 
                                                                            '$lang', 
                                                                            '$seq_num', 
                                                      									    '$user_id', 
                                                                            '$email', 
                                                      									    'BK', 
                                                                            'NS', 
                                                                            'ENS', 
                                                                            'Not subscribed eDM service', 
                                                                            '$is_trial', 
                                                                            '$delivery_datetime', 
                                                                            '$delivery_datetime')";

                 $result = mysqli_query( $cobrand['link'] , $insert_not_subscribed_email_sql);

           }

      }


      // IF -- Invalid Format (frontend reported as incorrect emails)
      // elog ("COUNT incorrect list : " . count($return_lists['incorrect_email_list']) . "\n\n");
      if ( count($return_lists['incorrect_email_list']) > 0 ) {

           foreach ($return_lists['incorrect_email_list'] as $key => $user ) {

            		$user_id = isset($user['id']) ? $user['id'] : '';
        		    $email = $user['prefixed_email'];
        		    //$lang = $user['send_lang'];
                $lang = isset($user['send_lang']) ? $user['send_lang'] : '';   // sometimes api comes in duplicate has not this lang value
		            $desc_code = $user['desc_code'];

                $insert_incorrect_email_sql = "INSERT INTO edm_stats (`campaign_id`, 
                                                                      `batch_id`, 
                                                                      `lang`, 
                                                                      `release_sequence`,
                                                								      `user_id`, 
                                                                      `email`, 
                                                								      `delivery_status`, 
                                                                      `delivery_failure_code`, 
                                                                      `desc_code`, 
                                                								      `description`, 
                                                                      `is_trial`, 
                                                                      `created`, 
                                                                      `modified`) 
            					  	       	                            VALUES ('$campaign_id', 
                                                                      '$batch_id', 
                                                                      '$lang', 
                                                                      '$seq_num', 
                                                                      '$user_id', 
                                                                      '$email', 
                                                                      'BK', 
                                                                      'IF', 
                                                                      '$desc_code', 
                                                                      'Incorrect email format', 
                                                                      '$is_trial', 
                                                                      '$delivery_datetime', 
                                                                      '$delivery_datetime')";

                $result = mysqli_query( $cobrand['link'], $insert_incorrect_email_sql );
                // elog ("Insert Incorrect SQL : $insert_incorrect_email_sql");

           }

      }


      // DE -- Duplicate Email
      if ( count($return_lists['duplicate_email_list']) > 0 ) {

           foreach ($return_lists['duplicate_email_list'] as $key => $user ) {

            		 $user_id = isset($user['id']) ? $user['id'] : '';
        		     $email = $user['prefixed_email'];
        		     //$lang = $user['send_lang'];
        		     $lang = isset($user['send_lang']) ? $user['send_lang'] : '';   // sometimes api comes in duplicate has not this lang value
        		     elog (">>>>> duplicate lang : $lang");

                 $insert_duplicate_email_sql = "INSERT INTO edm_stats (`campaign_id`, 
                                                                       `batch_id`, 
                                                                       `lang`, 
                                                                       `release_sequence`,
                                                								       `user_id`, 
                                                                       `email`, 
                                                								       `delivery_status`, 
                                                                       `delivery_failure_code`, 
                                                                       `desc_code`, 
                                                								       `description`, 
                                                                       `is_trial`, 
                                                                       `created`, 
                                                                       `modified`) 
                                                							 VALUES ('$campaign_id', 
                                                                       '$batch_id', 
                                                                       '$lang', 
                                                                       '$seq_num', 
                                                								       '$user_id', 
                                                                       '$email', 
                                                								       'BK', 
                                                                       'DE', 
                                                                       'DE', 
                                                	                     'Duplicate email address, system does not send twice', 
                                                                       '$is_trial', 
                                                                       '$delivery_datetime', 
                                                                       '$delivery_datetime')";


                 $result = mysqli_query( $cobrand['link'] , $insert_duplicate_email_sql );

                 //elog ("Insert Duplicate SQL : $insert_duplicate_email_sql");
                 //error_log ("sql : $insert_duplicate_email_sql");

           }

      }

      // No Status Yet -- Valid Sent
      if ( count($return_lists['sendout_email_list']) > 0 ) {

           foreach ($return_lists['sendout_email_list'] as $key => $user ) {

                 $user_id = isset($user['id']) ? $user['id'] : '';
                 $email = $user['prefixed_email'];
            		 $lang = $user['send_lang'];
                 $params = '';

            		 elog (">>>>> send out lang : $lang");
            		 //elog (">>>>> user id : $user_id, email : $email, here has params hash : " . $user['params_hash']);

            		 if ( isset($user['params_hash']) ) {
            	
            		      //elog ("here has params hash");
                      $params = ufo_encode('edm', $user['params_hash']);
                      $params = htmlspecialchars_decode($params);
            		      //elog ("params : $params ");
            		 }

                 $insert_valid_email_sql = "INSERT INTO edm_stats (`campaign_id`, 
                                                                   `batch_id`, 
                                                                   `lang`, 
                                                                   `release_sequence`,
                                                								   `user_id`, 
                                                                   `email`, 
                                                                   `params`, 
                                                								   `is_trial`, 
                                                                   `created`, 
                                                                   `modified`)
                                                           VALUES ('$campaign_id', 
                                                                   '$batch_id', 
                                                                   '$lang', 
                                                                   '$seq_num',
                                                								   '$user_id', 
                                                                   '$email', 
                                                                   '$params', 
                                                								   '$is_trial', 
                                                                   '$delivery_datetime', 
                                                                   '$delivery_datetime')";

                 $result = mysqli_query( $cobrand['link'] , $insert_valid_email_sql );

                 //elog ("Insert Valid SQL : $insert_valid_email_sql");
                 //elog ("Insert Valid SQL : $insert_valid_email_sql");
                 //error_log ("sql : $insert_duplicate_email_sql");

           }

      }

      mysqli_commit($cobrand['link']);
      mysqli_autocommit($cobrand['link'], TRUE);

      return array (1, 'OK');

}


/************************************ Report Functions ***************************************************/


function getReportByCampaignID ($campaign_id) {

     global $cobrand;
 
     $batches = _get_campaign_batches ($campaign_id);

     foreach ( $batches as $batch_id ) {
               $campaign_report[$batch_id] = _get_report_by_batch_id ($batch_id);
     }

     return $campaign_report;

}


function getReportByBatchID ($batch_id) {

     global $cobrand;

     $batch_report = _get_report_by_batch_id ($batch_id);

     return $batch_report;

}

function _get_campaign_batches ($campaign_id) {

     global $cobrand;

     $batches = array();

     $get_batches_sql = "SELECT batch_id FROM batch_breakdowns WHERE campaign_id = '$campaign_id'";
     $get_batches_result = mysqli_query( $get_batches_sql );

     while ( $ar = mysqli_fetch_assoc($get_batches_result) ) {
             array_push($batches, $ar['batch_id']);
     }

     return $batches;

}


function getSendProgress ($hostname, $campaign_id, $batch_id, $post_datetime) {

     global $cobrand;

     $progress_status = array();

     // Get different send file order & eDM server for this Campaign & Batch
     $get_batch_breakdown_sql = "SELECT edm_server,
                                        file_order,
                                        send_size,
                                        report1,
                                        report2
                                   FROM batch_breakdowns
                                  WHERE campaign_id = '$campaign_id'
                                    AND batch_id = '$batch_id'";

     //$get_batch_breakdown_result = mysqli_query( $cobrand['link'] , $get_batch_breakdown_sql );
     //elog("CID : $campaign_id , BID : $batch_id, Query : $get_batch_breakdown_sql");

     if ($get_batch_breakdown_result = mysqli_query($cobrand['link'], $get_batch_breakdown_sql )) {

       	 while ( $ar = mysqli_fetch_assoc($get_batch_breakdown_result) ) {

               $file_order = $ar['file_order'];
               //echo "File Order : $file_order<BR>";

  	           // Send Sequence is the current position it's been sending to in this file order
               if ( !empty( $ar['report1'] ) ) {

                    $send_sequence = $ar['send_size'];
                    $finish_time = $ar['report1'];

               } else {

                    $send_sequence = _query_remote_send_progress( $ar['edm_server'], $hostname, $cobrand['db_cfg']['mysql_database'],
  								  $campaign_id, $batch_id, $post_datetime, $file_order );
                    $finish_time = '';

               }

  	           elog ("send sequence : $send_sequence");
               $progress_status[$file_order]['edm_server'] = $ar['edm_server'];

               $server_num = $cobrand['edm_servers'][$ar['edm_server']]['server_num'];

               //$server_num = $ar['edm_server'];
               //$server_num = str_replace("inmartsmtp", "", $server_num);
               //$server_num = str_replace(".hkqmail.com", "", $server_num);

               $progress_status[$file_order]['server_num'] = $server_num;
               $progress_status[$file_order]['send_sequence'] = $send_sequence;
               $progress_status[$file_order]['send_size'] = $ar['send_size'];
               $progress_status[$file_order]['finish_time'] = $finish_time;

       	}

     }

     return $progress_status;

}


function _query_remote_send_progress ($edm_server, $hostname, $db_name, $campaign_id, $batch_id, $post_datetime, $file_order ) {

    global $cobrand;

    $bb_version = $cobrand['edm_servers'][$edm_server]['bb_version'];
    //$hostname = $cobrand['db_cfg']['mysql_database'] . ':' . $hostname;  // for single domain to distinguish database
    $param = "hostname=$hostname&cn=$db_name&campaign_id=$campaign_id&batch_id=$batch_id&post_datetime=$post_datetime&file_order=$file_order";

    elog("query remote send progress bb version : $bb_version");
    elog("Param : $param");

    $cobrand['solr_ch'] = curl_init();
    $script = 'http://' . $edm_server . '/' . 'progress_status_' . $bb_version . '.php';
    list($rc, $rm, $rec_data) = curl_call ('edm', $cobrand['solr_ch'], 'string', $script, $param);
    curl_close ($cobrand['solr_ch']);

    // echo "Rec Data (_query_remote_send_progress) : $rec_data<BR>";
    return $rec_data;

}


function check_quota_balance ($service_plan, $num_users_sent, $schedule_send, $schedule_datetime) {

    global $cobrand;

    $rc = 0;
    $rm = '';

    if ( $service_plan == 'TRI' || $service_plan == 'AVS' ) {

	       $month_id = _get_month_id ('dummy');   // year '0', month '0'
         $year = 0;
         $month = 0;

    } else {

      	 if ( $schedule_send ) { 

      	      $schedule_date = substr($schedule_datetime, 0, 10);
      	      $month_id = _get_month_id ($schedule_date);

              $year = substr($schedule_datetime, 0, 4);
              $month = substr($schedule_datetime, 5, 2);

      	 } else {

           	  list ($current_datetime, $current_year, $current_month, $current_day, $current_hour) = get_current_datetime();
      	      $current_date = substr($current_datetime, 0, 10);
      	      $month_id = _get_month_id ($current_date);

              $year = $current_year;
              $month = $current_month;

      	 }

      	 if ( $month < 10 && strlen($month) == 2 ) { $month = substr($month, 1, 1); }

    }

    // Note : Tmp Quota Means Sending Out a Non-Scheduled, Need to Deduct Quota First
    // When Stats. Come Back It Will Remove
    $get_remaining_quota_sql = "SELECT this_month_quota, 
                                       this_month_used, 
                                       this_month_scheduled, 
                                       this_month_remains
                      				    FROM monthly_reports
                      				   WHERE id = '$month_id'";

    elog ("get sql : $get_remaining_quota_sql");
    $get_remaining_quota_result = mysqli_query($cobrand['link'], $get_remaining_quota_sql);

    $row = mysqli_fetch_assoc($get_remaining_quota_result);
    //$remaining_quota = $row['this_month_remains'] - $row['this_month_scheduled'];  // Count also reserved for schedules
    $remaining_quota = $row['this_month_remains'];

    elog("remaining quota : $remaining_quota, num users sent : $num_users_sent");

    // Calculate Credits to Use
    $new_remaining_quota = $remaining_quota - $num_users_sent;

    elog ("remaining quota ($year-$month) : $remaining_quota");
    elog ("num users sent : $num_users_sent");
    elog ("new remaining quota : $new_remaining_quota\n\n");

    if ( $new_remaining_quota < 0 ) {
         return array ('0', 'Insufficient quota in your account', $remaining_quota, $new_remaining_quota);
    }

    elog ("quota pass ... return 1 \n\n");
      
    return array (1, 'OK', $remaining_quota, $new_remaining_quota);

}


// In eDM, Deduct Quota means add to reserve schedule fields of the scheduled month, not actual deduct 
function deduct_quota ($service_plan, $schedule_send, $schedule_send_id, $schedule_datetime, $schedule_sequence, $campaign_id, $batch_id, $num_imported_users, $num_input_users, $num_users_sent, $mockup, $debug) {

    global $cobrand;

    if ( $num_imported_users == '' ) { $num_imported_users = 0; }
    elog ("deduct quota num of imported users : $num_imported_users");
    //exit;

    /************************* Get Latest Release / Mockup Sequence ************/

    if ( $schedule_send ) { 

	       $delivery_datetime = $schedule_datetime;

    } else {

      	 // coz not a schedule send, delivery datetime is the current datetime
      	 list ($delivery_datetime, $delivery_year, $delivery_month, $delivery_day, $delivery_hour) = get_current_datetime();

    }

    $delivery_date = substr($delivery_datetime, 0, 10);
    $year = substr($delivery_datetime, 0, 4);
    $month = substr($delivery_datetime, 5, 2);

    $data['num_imported_users'] = $num_imported_users;
    $data['num_users_sent'] = $num_users_sent;
    $data['schedule_sequence'] = $schedule_sequence;

    if ( $batch_id >= 0 ) {  // Normal campaign needs to update batches, AR (-ve campaign_id) no need, but update in edm_finalize

       	 // Write to batches
       	 elog("schedule send : $schedule_send ... ");
       	 elog("num users sent : $num_users_sent ... ");
       	 elog("Write to Batches Start (deduct quota) ... ");
       	 //$num_valid_sent = count($sendout_email_list);
       	 list ($rc, $rm) = _write_to_batches ($schedule_send, $schedule_send_id, $campaign_id, $batch_id, 
    		 $num_imported_users, $num_input_users, $num_users_sent, $data, $mockup, $debug);
       	 if ( !$rc ) { return array ($rc, $rm); }
       	 elog("Write to Batches End ... ");

    }

    // Write to monthly reports
    list ($rc, $rm) = _edm_update_monthly_report ('before_sendout', $schedule_send, '', $batch_id, $mockup, $data, $delivery_date, $year, $month);

    if ( $service_plan == 'TRI' || $service_plan == 'AVS' ) {
	       list ($rc, $rm) = _edm_update_monthly_report ('before_sendout', $schedule_send, '', $batch_id, $mockup, $data, 'dummy', '0', '0');
    } else {
	       list ($rc, $rm) = check_and_update_exceeded_quota ($delivery_date);     // Trial and AVS no need update this
    }

    return array (1, 'OK');
    // Write to accounts

}


function _edm_update_monthly_report ($mode, $schedule_send, $sbatch_hash, $batch_id, $mockup, $data, $delivery_date, $this_year, $this_month) {

    global $cobrand;

    if ( $mockup == 1 ) {
       	 return array (1, 'OK');
    }

    $rc = 0;
    $rm = '';

    if ( $this_month < 10 && strlen($this_month) == 2 ) { $this_month = substr($this_month, 1, 1); }

    $month_id = _get_month_id ($delivery_date);
    $num_users_sent = $data['num_users_sent'];

    elog ("edm update monthly report, delivery date : $delivery_date");
    elog ("edm update month id : $month_id");

    if ( $mode == 'before_sendout' ) {

         list ($current_datetime, $current_year, $current_month, $current_day, $current_hour) = get_current_datetime();

      	 $this_sendout_scheduled = 0;
      	 $this_sendout_used = 0;
      	 $new_schedule = 0;

         if ( $schedule_send ) {
      	      $this_sendout_scheduled = $num_users_sent;
      	      //if ( empty($data['schedule_sequence']) ) { $new_schedule = 1; }
      	      $new_schedule = 1;
         } else {
	            $this_sendout_used = $num_users_sent;
	       }

	       // update service period quota, note, below fields is following service period, not calendar months
         $update_service_period_quota_sql = "UPDATE monthly_reports 
                                                SET num_scheduled = num_scheduled + $new_schedule,
                                                    num_scheduled_users = num_scheduled_users + $this_sendout_scheduled,
                                                    num_effective_users = num_effective_users + $num_users_sent,
                                                    this_month_scheduled = this_month_scheduled + $this_sendout_scheduled,
                                                    this_month_used = this_month_used + $this_sendout_used,
                                                    this_month_remains = this_month_remains - $num_users_sent
                                              WHERE id = '$month_id'";

         $update_service_period_quota_result = mysqli_query($cobrand['link'], $update_service_period_quota_sql);
         elog ("\n\n Update Monthly Report SQL - Service Period ($mode) : \n\n$update_service_period_quota_sql");

    }


    if ( $mode == 'after_sendout' ) {

         $num_scheduled_users = 0;
         $additional_users = 0;
         $refund_users = 0;

      	 if ( $schedule_send ) {

              $num_scheduled_users = $sbatch_hash['num_scheduled_users'];
              $additional_users = $data['additional_users'];
              $refund_users = $data['refund_users'];

       	 }

         $update_monthly_report_sql = "UPDATE monthly_reports
                                          SET num_released = num_released + 1,
                                              num_scheduled_users = num_scheduled_users - $num_scheduled_users,
                                              num_effective_users = num_effective_users + $additional_users - $refund_users,
                                              this_month_used = this_month_used + $num_scheduled_users + $additional_users - $refund_users,
                                              this_month_remains = this_month_remains - $additional_users + $refund_users
                                        WHERE id = '$month_id'";
      
          elog ("Update Monthy Report SQL : $update_monthly_report_sql");
          mysqli_query($cobrand['link'], $update_monthly_report_sql);

    }

    //num_valid_sent = num_valid_sent + $num_valid_sent,
    //num_invalid_sent = num_invalid_sent + $num_invalid_sent,

    return array (1, 'OK');

}

// if frontend webmaster cancels schedule, it will call this
// $year , $month are the original schedule year and month, it's for deduction
// $mode value : cancel_schedule / expired_no_execute 
function cancel_schedule_claim_quota ($mode, $service_plan, $campaign_id, $batch_id, $schedule_send_id, $schedule_date, $year, $month) {

    global $cobrand;

    elog ("mode : $mode, service plan : $service_plan, schedule date : $schedule_date");

    $sbatch_hash = get_batch_info ('edm', $batch_id);
    list ($rc, $rm) = _claim_monthly_reports_quota ($schedule_date, $year, $month, $sbatch_hash);

    if ( $service_plan == 'TRI' || $service_plan == 'AVS' ) {
         list ($rc, $rm) = _claim_monthly_reports_quota ('dummy', 0, 0, $sbatch_hash);
    }


    // if change schedule, frontend will re-write batch new figures, so no need update here

    if ( $mode == 'cancel_schedule' ) {    // if expired no execute (not approved), no need delete batch

         // Delete Schedule
    	   $delete_batch_sql = "DELETE FROM batches WHERE id = '$batch_id'";
    	   $delete_batch_result = mysqli_query($cobrand['link'], $delete_batch_sql);

    }

    return array (1, 'OK');

}


function _claim_monthly_reports_quota ($schedule_date, $year, $month, $sbatch_hash) {

    global $cobrand;

    elog ("schedule date : $schedule_date");
    $month_id = _get_month_id ($schedule_date);

    $num_scheduled_users = $sbatch_hash['num_scheduled_users'];
    $num_effective_users = $sbatch_hash['num_effective_users'];   // should be same as $num_scheduled_users actually


    // Update Service Period 
    $update_service_period_quota_sql = "UPDATE monthly_reports SET
                                       num_scheduled = num_scheduled - 1,
                                       num_scheduled_users = num_scheduled_users - $num_scheduled_users,
                                       num_effective_users = num_effective_users - $num_effective_users,
                                       this_month_scheduled = this_month_scheduled - $num_scheduled_users,
                                       this_month_remains = this_month_remains + $num_scheduled_users
                                 WHERE id = '$month_id'";

    $update_service_period_quota_result = mysqli_query($cobrand['link'], $update_service_period_quota_sql);
    elog ("Update Monthly SQL - Service Period : $update_service_period_quota_sql");

    return array (1, 'OK');

}


// Every time change schedule, system delete the old one (also claim quota) and re-add again (by frontend)
function change_edm_schedule ($service_plan, $edm_exceed_quota_stop_send, $schedule_send_id, $new_schedule_num_users, $new_schedule_datetime) {

    global $cobrand;

    $ss_hash = get_scheduled_send ('edm', $schedule_send_id);
    $sbatch_hash = get_batch_info ('edm', $ss_hash['batch_id']);

    $original_schedule_date = substr($ss_hash['schedule_send_datetime'], 0, 10);
    $original_schedule_year = substr($ss_hash['schedule_send_datetime'], 0, 4);
    $original_schedule_month = substr($ss_hash['schedule_send_datetime'], 5, 2);
    $original_schedule_month_id = _get_month_id ($original_schedule_date);

    $new_schedule_date = substr($new_schedule_datetime, 0, 10);
    $new_schedule_month_id = _get_month_id ($new_schedule_date);

    // Original Cost
    $original_quota = $sbatch_hash['num_scheduled_users'];

    // New Cost
    $new_quota = $new_schedule_num_users;

    if ( ($new_schedule_month_id != $original_schedule_month_id) || ($new_quota > $original_quota) ) {

      	  // If Same Service Period => New Quota Must Be Greater Than Original Quota, Check Additional Quota
      	  if ( $new_schedule_month_id == $original_schedule_month_id ) {
               $quota_to_check = $new_quota - $original_quota;
      	  } else {
               $quota_to_check = $new_schedule_num_users;
      	  }

      	  list ($rc, $rm, $remaining_quota, $new_remaining_quota) = check_quota_balance ($service_plan, $quota_to_check, 1, $new_schedule_datetime);

          if ( !$rc && $edm_exceed_quota_stop_send ) {  return array (0, 'Insufficient Quota');  }

    }

    // Delete Old Schedule
    elog ("in change edm schedule");

    list ($rc, $rm) = cancel_schedule_claim_quota ('change_chedule', $service_plan, $ss_hash['campaign_id'], $ss_hash['batch_id'], $schedule_send_id, $original_schedule_date, $original_schedule_year, $original_schedule_month);

    return array ('1', 'OK');

}


function check_and_update_exceeded_quota ($delivery_date) {

    global $cobrand;

    // new version does not support cost tier, no need use this, so simply return
    // return array (0, 'Quota not exceeded, no need update cost tier string');

    $month_id = _get_month_id ($delivery_date);

    $get_remaining_quota_sql = "SELECT this_month_remains
                                  FROM monthly_reports
                                 WHERE id = '$month_id'";

    $get_remaining_quota_result = mysqli_query($cobrand['link'], $get_remaining_quota_sql);
    elog("get remaining sql : $get_remaining_quota_sql");

    $row = mysqli_fetch_assoc($get_remaining_quota_result);

    if ( $row['this_month_remains'] >= 0 ) {
         return array (0, 'Quota not exceeded, no need update cost tier string');
    }

    $exceed_quota = abs($row['this_month_remains']);

    /*************** Get rate card of this account and do calculation *************/
    $get_rate_card_sql = "SELECT * FROM pps_rate_cards WHERE service = 'edm' ORDER BY id";
    $get_rate_card_result = mysqli_query($cobrand['link'], $get_rate_card_sql);
           
    $i = 0;
    $ttl_tier_cost = 0;
    
    while ( $ar = mysqli_fetch_assoc($get_rate_card_result) ) {
    
            ++$i;
    
            $this_tier_num = $ar['num_to'] - $ar['num_from'] + 1;

            if ( $exceed_quota >= $this_tier_num ) {
                 $this_tier_hit = $this_tier_num;
            } else {
                 $this_tier_hit = $exceed_quota;
            }

            $tier[$i]['num_from'] = $ar['num_from'];
            $tier[$i]['num_to'] = $ar['num_to'];
            $tier[$i]['unit_cost'] = $ar['price'];
            $tier[$i]['this_tier_hit'] = $this_tier_hit;
            $sub_total = $this_tier_hit * $ar['price'];
            $tier[$i]['sub_total'] = $sub_total;
            //$tier[$i]['sub_total'] = number_format($sub_total,3);

            $exceed_quota = $exceed_quota - $this_tier_hit;
            $ttl_tier_cost = $ttl_tier_cost + $tier[$i]['sub_total'];

    }

    $cost_tier_string = serialize($tier);

    /***************** Update monthly report ***************************************/
    $update_monthly_report_sql = "UPDATE monthly_reports
				                             SET this_month_extra_cost = '$ttl_tier_cost',
					                               cost_tier_string = '$cost_tier_string'
                                   WHERE id = '$month_id'";

    elog("cost tier sql : $update_monthly_report_sql");
    mysqli_query($cobrand['link'], $update_monthly_report_sql);

    return array (1, 'Quota exceeded, updated monthly quota cost tier string');

}


function _do_solr_filter (&$filter, $email, &$stat_figures) {

    global $cobrand;

    $need_exclude = 0;
    $need_exclude = _filter_responses ($filter, $email, $stat_figures);

    return $need_exclude;

}


// The mechanism to do filter is, frontend passes $filter parameter to here
// here is to check if matches criteria, if not, filter him out

function _filter_responses (&$filter, $email, &$stat_figures) {

   global $cobrand;

   $db_name = $cobrand['db_cfg']['mysql_database'];
   $need_exclude = 0;

   $phash = null;
   $phash['condition'] = array();
   $tmp_stmt = $tmp_stmt2 = $tmp_stmt3 = $tmp_stmt4 = $link_stmt = '';


   if (isset($filter['edm_reads'])) {

       // base collection: collection_edm_reads
       $phash['collection'] = 'edm_reads';

       // suppose filter users who have read campaign_id = 123 or campaign_id = 234
       foreach ( $filter['edm_reads'] as $array ) {

      	   if ( !empty($array['campaign_id']) ) {
                 $tmp_stmt .= ' OR (campaign_id:"' . $array['campaign_id'] . '")';
      	   }
        
        }

      	if ( !empty($tmp_stmt) ) { 
                   $tmp_stmt = preg_replace ('/^ OR /', '', $tmp_stmt);
      	     array_push($phash['condition'], ' AND (' . $tmp_stmt . ')'); 
      	}

        array_push($phash['condition'], ' AND (email:"' . $email . '")');
        array_push($phash['condition'], ' AND (release_sequence:{0 TO *})');  // avoid mockup also count

        if (isset($filter['edm_links'])) {

            // suppose also filter users who have clicked campaign_id = 345 and link = "http://www.google.com"
      	    // if chosen 'any' campaign, then no need campign id statement
      	    // if chosen campaign's 'any' link, then no need link statement

            foreach ( $filter['edm_links'] as $array ) {

              	  if ( !empty($array['campaign_id']) ) {   // has campaign id

              	       if ( !empty($array['link']) ) {   // has link
              		          $array['link'] = preg_replace ('/:/', '\:', $array['link']);
              			        $link_stmt = '" AND link:"' . $array['link'];
              		     } 

                       $tmp_stmt2 .= ' OR (campaign_id:"' . $array['campaign_id'] . $link_stmt . '")';

              		}

            }

      	    if ( !empty($tmp_stmt2) ) { 
                 $tmp_stmt2 = preg_replace ('/^ OR /', '', $tmp_stmt2);
  	             $tmp_stmt2 = " AND ($tmp_stmt2)";
  	        }

            $phash['fq'][0] = "{!join from=email to=email fromIndex=collection_edm_links} database_name:\"$db_name\" AND release_sequence:{0 TO *}" . $tmp_stmt2;

        }

        if (isset($filter['edm_not_reads'])) {

            // suppose also filter users who have NOT read campaign_id 456

            foreach ( $filter['edm_not_reads'] as $array ) {
                $tmp_stmt3 .= ' OR (campaign_id:"' . $array['campaign_id'] . '")';
            }

	          if ( !empty($tmp_stmt3) ) { 
                  $tmp_stmt3 = preg_replace ('/^ OR /', '', $tmp_stmt3);
		              $tmp_stmt3 = " AND ($tmp_stmt3)";
	          }

            $phash['edmNotReadQuery'] = "database_name:\"$db_name\" AND release_sequence:{0 TO *}" . $tmp_stmt3;
            $phash['fq'][1] = "-({!join from=email to=email fromIndex=collection_edm_reads v=\$edmNotReadQuery})";

        }
    
        if (isset($filter['edm_not_links'])) {

            // suppose also filter users who have NOT clicked campaign_id 567 and link = "http://hk.yahoo.com"

            foreach ( $filter['edm_not_links'] as $array ) {

                if ( !empty($array['campaign_id']) ) {   // has campaign id

                     if ( !empty($array['link']) ) {   // has link
                          $array['link'] = preg_replace ('/:/', '\:', $array['link']);
                          $link_stmt = '" AND link:"' . $array['link']; 
                     } 

                     $tmp_stmt4 .= ' OR (campaign_id:"' . $array['campaign_id'] . $link_stmt . '")';

                }

            }

        	  if ( !empty($tmp_stmt4) ) { 
                 $tmp_stmt4 = preg_replace ('/^ OR /', '', $tmp_stmt4);
		             $tmp_stmt4 = " AND ($tmp_stmt4)";
	          }

            $phash['edmNotLinkQuery'] = "database_name:\"$db_name\" AND release_sequence:{0 TO *}" . $tmp_stmt4;
            $phash['fq'][2] = "-({!join from=email to=email fromIndex=collection_edm_links v=\$edmNotLinkQuery})";

        }


   } else if (isset($filter['edm_links'])) {

	      // base collection: collection_edm_links
        $phash['collection'] = 'edm_links';

	      // suppose filter users who have clicked campaign_id = 345 and link = "http://www.google.com"

        foreach ( $filter['edm_links'] as $array ) {

             if ( !empty($array['campaign_id']) ) {

                  if ( !empty($array['link']) ) {   // has link
                       $array['link'] = preg_replace ('/:/', '\:', $array['link']);
                       $link_stmt = '" AND link:"' . $array['link']; 
                  } 

                  $tmp_stmt .= ' OR (campaign_id:"' . $array['campaign_id'] . '")';

             }

        }

        if ( !empty($tmp_stmt) ) {
             $tmp_stmt = preg_replace ('/^ OR /', '', $tmp_stmt);
             array_push($phash['condition'], ' AND (' . $tmp_stmt . ')');
        }

        array_push($phash['condition'], ' AND (email:"' . $email . '")');
        array_push($phash['condition'], ' AND (release_sequence:{0 TO *})');  // avoid mockup also count


        if (isset($filter['edm_not_reads'])) {

	          // suppose also filter users who have NOT read campaign_id 456
            foreach ( $filter['edm_not_reads'] as $array ) {

                if ( !empty($array['campaign_id']) ) {
                     $tmp_stmt2 .= ' OR (campaign_id:"' . $array['campaign_id'] . '")';
                }

            }

            if ( !empty($tmp_stmt2) ) {
                 $tmp_stmt2 = preg_replace ('/^ OR /', '', $tmp_stmt2);
            		 $tmp_stmt2 = " AND ($tmp_stmt2)";
	          }

	          $phash['edmNotReadQuery'] = "database_name:\"$db_name\" AND release_sequence:{0 TO *}" . $tmp_stmt2;
	          $phash['fq'][0] = "-({!join from=email to=email fromIndex=collection_edm_reads v=\$edmNotReadQuery})";

	      }
	
	      if (isset($filter['edm_not_links'])) {

	          // suppose also filter users who have NOT clicked campaign_id 567 and link = "http://hk.yahoo.com"
            foreach ( $filter['edm_not_links'] as $array ) {

                if ( !empty($array['campaign_id']) ) {

                     if ( !empty($array['link']) ) {   // has link
                          $array['link'] = preg_replace ('/:/', '\:', $array['link']);
                          $link_stmt = '" AND link:"' . $array['link'];
                     }

                     $tmp_stmt3 .= ' OR (campaign_id:"' . $array['campaign_id'] . $link_stmt . '")';

                }

            }

            if ( !empty($tmp_stmt3) ) {
                 $tmp_stmt3 = preg_replace ('/^ OR /', '', $tmp_stmt3);
                 $tmp_stmt3 = " AND ($tmp_stmt3)";
            }

            $phash['edmNotLinkQuery'] = "database_name:\"$db_name\" AND release_sequence:{0 TO *}" . $tmp_stmt3;
	          $phash['fq'][1] = "-({!join from=email to=email fromIndex=collection_edm_links v=\$edmNotLinkQuery})";

	      }


   } else if (isset($filter['edm_not_reads'])) {

        // base collection: collection_edm_reads
        $phash['collection'] = 'edm_reads';

	      // suppose filter users who have NOT read campaign_id 456

        foreach ( $filter['edm_not_reads'] as $array ) {
        
             if ( !empty($array['campaign_id']) ) {
                  $tmp_stmt .= ' OR (campaign_id:"' . $array['campaign_id'] . '")';
             }
        
        }

        if ( !empty($tmp_stmt) ) {
             $tmp_stmt = preg_replace ('/^ OR /', '', $tmp_stmt);
             $tmp_stmt = " AND ($tmp_stmt)";
        }

        array_push($phash['condition'], ' AND (email:"' . $email . '")');
        array_push($phash['condition'], ' AND (release_sequence:{0 TO *})');  // avoid mockup also count

      	//$phash['edmNotReadQuery'] = "database_name:\"$db_name\" AND release_sequence:{0 TO *} AND ($tmp_stmt)";
  	    $phash['edmNotReadQuery'] = "database_name:\"$db_name\" AND release_sequence:{0 TO *}" . $tmp_stmt;
  	    $phash['fq'][0] = "-({!join from=email to=email fromIndex=collection_edm_reads v=\$edmNotReadQuery})";
	
	      if (isset($filter['edm_not_links'])) {

	          // suppose also filter users who have NOT clicked campaign_id 567 and link = "http://hk.yahoo.com"

            foreach ( $filter['edm_not_links'] as $array ) {

                if ( !empty($array['campaign_id']) ) {

                     if ( !empty($array['link']) ) {   // has link
                          $array['link'] = preg_replace ('/:/', '\:', $array['link']);
                          $link_stmt = '" AND link:"' . $array['link'];
                     }

                     $tmp_stmt2 .= ' OR (campaign_id:"' . $array['campaign_id'] . $link_stmt . '")';

                }

            }

            if ( !empty($tmp_stmt2) ) {
                 $tmp_stmt2 = preg_replace ('/^ OR /', '', $tmp_stmt2);
                 $tmp_stmt2 = " AND ($tmp_stmt2)";
            }

	          $phash['edmNotLinkQuery'] = "database_name:\"$db_name\" AND release_sequence:{0 TO *}" . $tmp_stmt2;
	          $phash['fq'][1] = "-({!join from=email to=email fromIndex=collection_edm_links v=\$edmNotLinkQuery})";

	       }

   } else if (isset($filter['edm_not_links'])) {

	      // base collection: collection_edm_links
        $phash['collection'] = 'edm_links';
        $phash['remark'] = 'filter edm not clicked';

	      // suppose filter users who have NOT clicked campaign_id 567 and link = "http://hk.yahoo.com"
        foreach ( $filter['edm_not_links'] as $array ) {

             if ( !empty($array['campaign_id']) ) {

                  if ( !empty($array['link']) ) {   // has link
                       $array['link'] = preg_replace ('/:/', '\:', $array['link']);
                       $link_stmt = '" AND link:"' . $array['link'];
                  }

                  $tmp_stmt .= ' OR (campaign_id:"' . $array['campaign_id'] . '")';

             }

        }

        if ( !empty($tmp_stmt) ) {
             $tmp_stmt = preg_replace ('/^ OR /', '', $tmp_stmt);
             $tmp_stmt = " AND ($tmp_stmt)";
        }


        array_push($phash['condition'], ' AND (email:"' . $email . '")');
        array_push($phash['condition'], ' AND (release_sequence:{0 TO *})');  // avoid mockup also count

      	$phash['edmNotLinkQuery'] = "database_name:\"$db_name\" AND release_sequence:{0 TO *}" . $tmp_stmt;
	      $phash['fq'][0] = "-({!join from=email to=email fromIndex=collection_edm_links v=\$edmNotLinkQuery})";

   } else {

	     return 0;

   }

   $phash['solr_group_field'] = 'email';
   $phash['solr_limit'] = '0';

   list ($rc, $rm, $num_records, $solr_hash) = connect_solr ('edm', $cobrand['solr_ch'], 'select', $phash);

   // Some does't not read but actually not responded to any campaigns, should not mistakenly count them in
   if ( $num_records == 0 ) {   // does not match criteria

        if (isset($filter['edm_not_reads'])) {    // need verify he's in edm_reads before, if not, don't filter out

            // elog ("Do additional edm_reads query");
            $phash = array();
            $phash['collection'] = 'edm_reads';
            $phash['condition'] = array();
            $phash['solr_limit'] = '0';

            array_push($phash['condition'], ' AND (email:"' . $email . '")');
            array_push($phash['condition'], ' AND (release_sequence:{0 TO *})');

            list ($rc, $rm, $num_records2, $solr_hash) = connect_solr ('edm', $cobrand['solr_ch'], 'select', $phash);
            if ( $num_records2 == 0 )  { return 0; }  // not bingo, don't filter

        }

        if (isset($filter['edm_not_links'])) {   // need verify he's in edm_clicks before, if not, don't filter out

            // elog ("Do additional edm_links query");
            $phash = array();
            $phash['collection'] = 'edm_links';
            $phash['condition'] = array();
            $phash['solr_limit'] = '0';

            array_push($phash['condition'], ' AND (email:"' . $email . '")');
            array_push($phash['condition'], ' AND (release_sequence:{0 TO *})');

            list ($rc, $rm, $num_records2, $solr_hash) = connect_solr ('edm', $cobrand['solr_ch'], 'select', $phash);
            if ( $num_records2 == 0 )  { return 0; }  // not bingo, don't filter

        }

   }

   // this user really does not match criteria, can filter out
   if ( $num_records == 0 ) {   // does not match criteria
        ++$stat_figures['num_filtered'];
        $need_exclude = 1;
   }

   return $need_exclude;

}



// This is global for all clients to use, stored in each server's ufo_local database
function _get_dead_emails_list () {

   global $cobrand;

   include_once("/etc/ufosend/available_servers.php");

   slog("This IP : $this_ip");
   $sites_mysql_admin = $servers[$this_ip]['2'];
   list ($mysql_username, $mysql_password) = explode (':', $sites_mysql_admin);

   slog (" username : " . $mysql_username . " , password : " . $mysql_password);

   $link = mysqli_connect ('localhost', $mysql_username, $mysql_password, 'ufo_local');
   mysqli_set_charset($link, "utf8");

   $dead_emails_list = array();

   $get_dead_emails_list_sql = "SELECT email FROM dead_emails";
   $get_dead_emails_list_result = mysqli_query($link, $get_dead_emails_list_sql);

   if ( mysqli_num_rows($get_dead_emails_list_result) > 0 ) {

        while ( $ar = mysqli_fetch_assoc($get_dead_emails_list_result) ) {
                $email = $ar['email'];
                $dead_emails_list[$email] = $email;
                elog (" dead email : $email");
        }

   }

   mysqli_close($link);

   return $dead_emails_list;

}


// get which month id this date relies in
function _get_month_id ($date) {

   global $cobrand;

   $month_id = '';

   if ( $date == 'dummy' ) {

        $get_month_id_sql = "SELECT id FROM monthly_reports WHERE year = '0' and month = '0' AND service = 'edm'";

   } else {

        $get_month_id_sql = "SELECT id FROM monthly_reports WHERE `effective_start_date` <= '$date' AND
                             `effective_end_date` >= '$date' AND active = 1 AND service = 'edm' 
			                       ORDER BY id DESC LIMIT 1";

   }

   elog ("get month id sql : $get_month_id_sql");
   $get_month_id_result = mysqli_query($cobrand['link'], $get_month_id_sql);
   $ar = mysqli_fetch_assoc($get_month_id_result);
   $month_id = $ar['id'];

   return $month_id;

}


// get campaign report
function api_get_edm_campaign_report ( &$tmp_hash )
{
   global $cobrand;

   $cobrand['solr_ch'] = curl_init();
   $stats = array();

   $campaign_id = $tmp_hash['campaign_id'];

   $verify_campaign_sql = "SELECT id FROM campaigns WHERE id = '$campaign_id'";
   $verify_campaign_result = mysqli_query($cobrand['link'], $verify_campaign_sql);

   
   if ( mysqli_num_rows($verify_campaign_result) == 0 ) {
        api_report_response ('sms', 0, -100, 'Incorrect campaign_id', '');
   }

   // get sum of all batches of this campaign
   $get_batch_sql = "SELECT SUM(num_effective_users) AS num_effective_users,
                            SUM(num_users_sent) AS num_users_sent
                       FROM batches WHERE campaign_id = '$campaign_id' AND release_sequence > 0";

   $get_batch_result = mysqli_query($cobrand['link'], $get_batch_sql);


   $row = mysqli_fetch_assoc($get_batch_result);
   $num_users_sent = $row['num_users_sent'];
   $response['campaign_id'] = $campaign_id;
   $response['num_users_sent'] = $row['num_users_sent'];

   // prepare basic share query settings
   $response = api_get_report ('edm', $tmp_hash, $response); 

   curl_close ($cobrand['solr_ch']);

   return array (1, 'OK', $response);

}


function api_get_edm_batch_report ( &$tmp_hash )
{

   global $cobrand;

   $cobrand['solr_ch'] = curl_init();
   $response = array();  // report array

   $campaign_id = $tmp_hash['campaign_id'];
   $release_num = $tmp_hash['release_num'];

   // get batch id
   $get_batch_sql = "SELECT id, send_method, num_effective_users, num_users_sent, post_datetime FROM batches WHERE campaign_id = '$campaign_id' AND release_sequence = '$release_num'";
   $get_batch_result = mysqli_query($cobrand['link'], $get_batch_sql);

   //elog("get batch sql : $get_batch_sql");
   if ( mysqli_num_rows($get_batch_result) == 0 ) {
        api_report_response ('edm', 0, -100, 'Incorrect campaign_id / release no. pair', '');
   }

   $row = mysqli_fetch_assoc($get_batch_result);
   $response['campaign_id'] = $campaign_id;
   $response['release_num'] = $release_num;
   $response['num_users_sent'] = $row['num_users_sent'];
   $response['send_method'] = $row['send_method'];
   $response['post_datetime'] = $row['post_datetime'];
   $tmp_hash['batch_id'] = $row['id']; // for api_get_report use

   // prepare basic share query settings
   $response = api_get_report ('edm', $tmp_hash, $response); 

   curl_close ($cobrand['solr_ch']);

   return array (1, 'OK', $response);

}


?>