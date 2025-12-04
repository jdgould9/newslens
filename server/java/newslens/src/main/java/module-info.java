module app.newslens {
    requires javafx.controls;
    requires javafx.fxml;


    opens app.newslens to javafx.fxml;
    exports app.newslens;
}