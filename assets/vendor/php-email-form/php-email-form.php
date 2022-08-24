<?php
class PHP_Email_Form {
  private bool $ajax;
  private string $toEmail;
  private string $from_name;
  private string $from_email;
  private string $subject;

  // Methods
  function add_message(...) {
  // write the code
  }

 function send() {
  // write the code
    $mailHeaders = "From: " . $_POST["userName"] . "<". $_POST["userEmail"] .">\r\n";
    if(mail($toEmail, $_POST["subject"], $_POST["content"], $mailHeaders)) {
        print "<p class='success'>Mail Sent.</p>";
    } else {
        print "<p class='Error'>Problem in Sending Mail.</p>";
    }
  }
}
?>
