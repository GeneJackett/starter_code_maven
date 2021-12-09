package cs3743;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.*;

////////////////////////////
// Adminer login
// Your login: ydh648
//Your pw: vXPNlWCLqs16uipFSnR0

public class P3Program 
{
    private Connection connect = null;
    
    private Statement statement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;
    public static final int ER_DUP_ENTRY = 1062;
    public static final int ER_DUP_ENTRY_WITH_KEY_NAME = 1586;
    public static final String[] strPropertyIdM =
    {   "MTNDDD"
       ,"NYCCC"
       ,"HOMEJJJ"
       ,"END"
    };
    
    public P3Program (String user, String password) throws Exception {
        try {
            // This will load the MySQL driver, each DBMS has its own driver
            MysqlDataSource ds = new MysqlDataSource();
            ds.setURL("jdbc:mysql://cs3743.fulgentcorp.com:3306/cs3743_" + user);
            ds.setUser(user);
            ds.setPassword(password);
            this.connect = ds.getConnection();
        } catch (Exception e) {
            throw e;
        } 
        
    }
       
    public void runProgram() throws Exception 
    {
        try 
        {
            // your code
            //crete select statement
            //Part A
            preparedStatement = connect.prepareStatement
                    ("select c.* from Customer c");
            resultSet = preparedStatement.executeQuery();
            //Part B
            printCustomers("Beginning Customers",resultSet);
            //Part C
            preparedStatement = connect.prepareStatement
                    ("select m.* from Property m");
            resultSet = preparedStatement.executeQuery();
            //Part D
            MySqlUtility.printUtility("Beginning Properties", resultSet);

            //preparedStatement = connect.prepareStatement("insert into cs3743_ydh648.Customer (custNr, name, baseLoc, birthDt, gender)" + "values (1999, Wesley, LA, 1992/07/18, M)");
            //Part E/F
            preparedStatement = connect.prepareStatement("insert into cs3743_ydh648.Customer(custNr, name, baseLoc, birthDt, gender)" + "values (?,?,?,?, ?)");
            int custNr = 1999;
            String name = "Wesley Jackson";
            String baseLoc = "LA";
            String birthDt = "1992-07-18";
            String gender = "M";

            preparedStatement.setInt(1,custNr);
            preparedStatement.setString(2,name);
            preparedStatement.setString(3,baseLoc);
            preparedStatement.setString(4,birthDt);
            preparedStatement.setString(5,gender);
            preparedStatement.executeUpdate();
        }
        catch (SQLException e)
        {
            switch (e.getErrorCode())
            {
                case ER_DUP_ENTRY:
                case ER_DUP_ENTRY_WITH_KEY_NAME:
                    System.out.printf("Duplicate key error: %s\n", e.getMessage());
                    break;
                default:
                    throw e;
            }
        }
        catch (Exception e)
        {
            throw e;
        }
        //PART E/F
        preparedStatement = connect.prepareStatement
                ("select c.* from Customer c");
        resultSet = preparedStatement.executeQuery();
        printCustomers("Customers after I was added ",resultSet);
        preparedStatement = connect.prepareStatement
                ("select TABLE_SCHEMA, TABLE_NAME, INDEX_NAME\n" +
                        ", SEQ_IN_INDEX, COLUMN_NAME, CARDINALITY\n" +
                        "from INFORMATION_SCHEMA.STATISTICS\n" +
                        "where TABLE_SCHEMA = 'cs3743_ydh648'\n" +
                        "and TABLE_NAME = \"Rental\"\n" +
                        "order by INDEX_NAME, SEQ_IN_INDEX;");
        resultSet = preparedStatement.executeQuery();
        MySqlUtility.printUtility("My Rental Indexes", resultSet);
        //Part H
        preparedStatement = connect.prepareStatement("insert into cs3743_ydh648.Rental(custNr, propId, startDt, totalCost)" + "values (?,?,?,?)");
        //Part I
        double totalCost = 200;
        for(int i = 1; i < strPropertyIdM.length; i++){
            if(strPropertyIdM[i].equals("END")){
                int custNr = 1999;
                String propId = strPropertyIdM[i];
                String startDt = "2019-12-14";
                preparedStatement.setInt(1,custNr);
                preparedStatement.setString(2,propId);
                preparedStatement.setString(3,startDt);
                preparedStatement.setDouble(4,totalCost);
                try {
                    preparedStatement.executeUpdate();
                }catch (SQLException e)
                    {
                        switch (e.getErrorCode())
                        {
                            case ER_DUP_ENTRY:
                            case ER_DUP_ENTRY_WITH_KEY_NAME:
                                System.out.printf("Duplicate key error: %s\n", e.getMessage());
                                break;
                            default:
                                throw e;
                        }
                    }
        catch (Exception e)
                    {
                        throw e;
                    }
                //adds 10 for each of the rentals
                totalCost += 10;
            }

        }
        //Part J
        preparedStatement = connect.prepareStatement
                ("select r.* from Rental r");
        resultSet = preparedStatement.executeQuery();

    }

    private void printCustomers(String title, ResultSet resultSet) throws SQLException
    {
       // Your output for this must match the format of my sample output exactly. 
       // custNr, name, baseLoc, birthDt, gender
        System.out.printf("%s\n", title);
        System.out.printf("   %-8s%-20s           %-5s %-11s %-2s\n"
                , "CustNr", "Name", "State", "Birth Dt", "Gender");
        // your code
        while (resultSet.next()) {
            // It is possible to get the columns via name
            // also possible to get the columns via the column number
            // which starts at 1

            //String timeStr;  // can be null

            int custNr = resultSet.getInt("custNr");
            String name = resultSet.getString("name");
            if (name == null)
                name = "---";
            String baseLoc = resultSet.getString("baseLoc");
            if (baseLoc == null)
                baseLoc = "---";
            String birthDt = resultSet.getString("birthDt");          // can be null
            if (birthDt == null) {
                birthDt = "---";
            }
            String gender = resultSet.getString("gender");      // can be null
            if (gender == null)
                gender = "---";
            System.out.printf("   %-8s%-20s           %-5s %-11s %-2s\n"
                    , custNr
                    , name
                    , baseLoc
                    , birthDt
                    , gender);
        }

        }
    

    // Close the resultSet, statement, preparedStatement, and connect
    private void close() 
    {
        try 
        {
            if (resultSet != null) 
                resultSet.close();

            if (statement != null) 
                statement.close();
            
            if (preparedStatement != null) 
                preparedStatement.close();

            if (connect != null) 
                connect.close();
        } 
        catch (Exception e) 
        {

        }
    }

}
