### 创建文件 `POST/api/fs/createFile`
```
path variable: none
request body: {
	"dst", //string 文件所在位置，绝对路径
	"content", //string 文件内容
}
```
```
response body: {
	"errCode", //integer
}
```
### 下载文件 `GET/api/fs/downloadFile`
```
path variable: 
filename, //string 文件名
dst, //string 文件所在位置（绝对路径，不包含文件名）
```
### 上传文件 `POST/api/fs/uploadFile`
```
path variable: 
dst, //string 上传文件的路径
request body: {
	"file", //Multipartfile form表单方式 
}
```
```
response body: {
	"errCode", //integer
}
```
### 更改文件（夹）名 `PUT/api/fs/rename`
```
path variable: none
request body: {
	"oldName", //string 文件所在位置+旧文件名，绝对路径
	"newName", //string 文件所在位置+新文件名，绝对路径
}
```
```
response body: {
	"errCode", //integer
}
```
### 删除文件（夹）`DELETE/api/fs/delete`
```
path variable: none
request body: {
	"filePath", //string 文件所在位置+文件名，绝对路径
}
```
```
response body: {
	"errCode", //integer
}
```
### 新建文件夹 `POST/api/fs/mkdir`
```
path variable: none
request body: {
	"path", //string 文件夹所在位置，绝对路径
}
```
```
response body: {
	"errCode", //integer
}
```
### 读取文件内容 `GET/api/fs/readFile`
```
path variable: 
filePath, //string 文件路径+文件名
```
```
response body: {
	"errCode", //integer
	"content", //string 文件内容
}
```
### 读取文件夹下所有文件（夹） `GET/api/fs/getDirectoryFromHdfs`
```
path variable: 
path, //string 文件夹路径
```
```
response body: {
	"errCode", //integer
	"directories": [{
    		"name": //string 文件名
    		"length", //long 文件大小
    		"path", //string 文件路径+文件名
    	}, ...],
}
```
