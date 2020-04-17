<!doctype html>
<html lang="en">
<head>
    <!-- Required meta tags -->
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">

    <!-- Bootstrap CSS -->
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css"
          integrity="sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh" crossorigin="anonymous">

    <title>状态查询</title>
</head>
<body>

<style>
    .bd-placeholder-img {
        font-size: 1.125rem;
        text-anchor: middle;
        -webkit-user-select: none;
        -moz-user-select: none;
        -ms-user-select: none;
        user-select: none;
    }

    @media (min-width: 768px) {
        .bd-placeholder-img-lg {
            font-size: 3.5rem;
        }
    }

    .table {
        margin-top: 50px;
    }
</style>
<div class="container">
    <div class="py-5 text-center">
        <img class="d-block mx-auto mb-4" src="/static/images/logo.png" alt="logo" height="72">
        <h2>乾坤大挪移</h2>
        <p class="lead">此功分七层境界，悟性高者修习，第一层需七年，第二层加倍，如此愈发困难。<br>此为本功法之运行状态，可观之以促功力</p>
        <table class="table">
            <thead>
            <tr>
                <th scope="col">#</th>
                <th scope="col">db-name</th>
                <th scope="col">table-name</th>
                <th scope="col">es-index</th>
                <th scope="col">kafka-topic</th>
                <th scope="col">kafka-group</th>
                <th scope="col">delay(s)</th>
                <th scope="col">state</th>
            </tr>
            </thead>
            <tbody>
            <#list data as d>
                <tr>
                    <th scope="row">${d.index}</th>
                    <td>${d.dbName}</td>
                    <td>${d.tableName}</td>
                    <td>${d.esIndexName}</td>
                    <td>${d.topicName}</td>
                    <td>${d.topicGroup}</td>
                    <td>${d.delay}</td>
                    <td>${d.state}</td>
                </tr>
            </#list>
            </tbody>
        </table>
    </div>

    <footer class="my-5 pt-5 text-muted text-center text-small">
        <p class="mb-1">© 2020-2028 Jte</p>
    </footer>

    <!-- Optional JavaScript -->
    <!-- jQuery first, then Popper.js, then Bootstrap JS -->
    <script src="https://code.jquery.com/jquery-3.4.1.slim.min.js"
            integrity="sha384-J6qa4849blE2+poT4WnyKhv5vZF5SrPo0iEjwBvKU7imGFAV0wwj1yYfoRSJoZ+n"
            crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js"
            integrity="sha384-Q6E9RHvbIyZFJoft+2mJbHaEWldlvI9IOYy5n3zV9zzTtmI3UksdQRVvoxMfooAo"
            crossorigin="anonymous"></script>
    <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.min.js"
            integrity="sha384-wfSDF2E50Y2D1uUdj0O3uMBJnjuUD4Ih7YwaYd1iqfktj0Uod8GCExl3Og8ifwB6"
            crossorigin="anonymous"></script>
</body>
</html>