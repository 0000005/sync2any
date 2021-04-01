<!doctype html>
<html lang="en">
<head>
    <!-- Required meta tags -->
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link href="https://cdn.bootcss.com/font-awesome/5.13.0/css/all.css" rel="stylesheet">
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
    .content{
        width: 1500px;
        margin: 0 auto;
    }
</style>
<div class="content">
    <div class="py-5 text-center">
        <img class="d-block mx-auto mb-4" src="/static/images/logo.png" alt="logo" height="72">
        <h2>sync2any</h2>
        <p class="lead">状态面板，所有同步任务都会在此列出。如果同步过程中有任何异常，应该先到此处进行排查。</p>
        <table class="table">
            <thead>
                <tr>
                    <th scope="col">#</th>
                    <th scope="col">db name</th>
                    <th scope="col">table name</th>
                    <th scope="col">es index</th>
                    <th scope="col">kafka topic</th>
                    <th scope="col">kafka group</th>
                    <th scope="col">
                        <div data-toggle="tooltip" title="最近一次同步时间">last update time(ms)
                            <i class="fa fa-info-circle" aria-hidden="true"></i>
                        </div>
                    </th>
                    <th scope="col">
                        <div data-toggle="tooltip" title="与数据源的延迟时间">delay(s)
                            <i class="fa fa-info-circle" aria-hidden="true"></i>
                        </div>
                    </th>
                    <th scope="col">
                        <div data-toggle="tooltip" title="处理一个Msg所花费的时间">tpq(ms)
                            <i class="fa fa-info-circle" aria-hidden="true"></i>
                        </div>
                    </th>
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
                    <td>${d.lastSyncTime}</td>
                    <td>${d.delay}</td>
                    <td>${d.tpq}</td>
                    <#if d.state == '已停止'>
                        <td><a href="javascript:showError('${d.errorReason}')">${d.state}</a></td>
                    <#else>
                        <td>${d.state}</td>
                    </#if>
                </tr>
            </#list>
            </tbody>
        </table>
    </div>

    <footer class="my-5 pt-5 text-muted text-center text-small">
        <p class="mb-1">© 2020 jte v1.0</p>
    </footer>

    <!-- Modal -->
    <div class="modal fade" id="myModal" tabindex="-1" role="dialog" aria-labelledby="exampleModalScrollableTitle" aria-hidden="true">
        <div class="modal-dialog modal-dialog-scrollable modal-xl" role="document">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="exampleModalScrollableTitle">错误原因</h5>
                    <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                        <span aria-hidden="true">&times;</span>
                    </button>
                </div>
                <div class="modal-body" id="errorContent">
                    ...
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-dismiss="modal">关闭</button>
                </div>
            </div>
        </div>
    </div>

    <script>
        function showError(msg)
        {
            $("#errorContent").html(msg);
            $('#myModal').modal('show')
        }
    </script>

    <!-- Optional JavaScript -->
    <!-- jQuery first, then Popper.js, then Bootstrap JS -->
    <script src="https://cdn.bootcss.com/jquery/3.4.1/jquery.slim.js"></script>
    <script src="https://cdn.bootcss.com/popper.js/1.16.0/esm/popper.min.js"></script>
    <script src="https://cdn.bootcss.com/twitter-bootstrap/4.4.1/js/bootstrap.min.js"></script>
</body>
</html>