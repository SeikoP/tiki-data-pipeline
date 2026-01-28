# Dự án Tiki Data Pipeline

Dữ liệu trên các sàn thương mại điện tử luôn biến động từng giây. Dự án này được xây dựng để giải quyết bài thách thức về việc thu thập và quản lý nguồn thông tin khổng lồ từ Tiki một cách tự động và bền vững. Mục tiêu cốt lõi là chuyển hóa những dữ liệu thô trên web thành một kho lưu trữ có cấu trúc, cho phép quan sát được sự thay đổi của thị trường theo thời gian.

## Cách thức vận hành và tổ chức dữ liệu

Thay vì thu thập dữ liệu một cách tuần tự và chậm chạp, hệ thống được thiết kế để phân rã công việc thành hàng trăm luồng xử lý độc lập. Cơ chế này cho phép pipeline tự điều chỉnh quy mô dựa trên số lượng danh mục sản phẩm đang tồn tại trên sàn. Mỗi nhóm sản phẩm được quản lý riêng biệt, đảm bảo rằng một sự cố nhỏ ở một danh mục sẽ không làm ảnh hưởng đến tiến độ chung của toàn bộ luồng công việc.

Dữ liệu sau khi thu thập sẽ đi qua một bộ lọc khắt khe để đảm bảo tính đồng nhất. Những thông tin nhiễu hoặc sai lệch được loại bỏ ngay từ đầu. Một điểm đặc biệt là sự tham gia của trí tuệ nhân tạo trong việc xử lý ngôn ngữ, giúp tinh gọn tên gọi của hàng ngàn sản phẩm, tạo ra một danh mục thông tin chuyên nghiệp và dễ dàng cho việc trình bày sau này.

## Giá trị từ sự tích lũy lịch sử

Sức mạnh thực sự của pipeline không chỉ nằm ở việc biết được hôm nay có gì, mà là việc ghi nhớ được quá khứ. Hệ thống ghi lại từng thay đổi nhỏ nhất về giá cả, số lượng bán và xếp hạng của sản phẩm. Sự tích lũy này tạo tiền đề cho việc phân tích xu hướng thị trường, giúp nhận diện được sự trồi sụt của các dòng sản phẩm và hành vi của người tiêu dùng qua từng giai đoạn thời gian cụ thể. Đây là nguồn tài nguyên quan trọng để chuyển từ việc quan sát sang việc thấu hiểu thị trường.

## Độ tin cậy và khả năng tự phục hồi

Môi trường mạng luôn tiềm ẩn nhiều rào cản. Do đó, pipeline được xây dựng với tư duy ưu tiên sự ổn định. Hệ thống sở hữu những cơ chế tự vệ để nhận biết khi nào cần tạm dừng để tránh các rủi ro từ phía máy chủ nguồn, đồng thời biết cách tự quay lại làm việc khi điều kiện cho phép. Việc quản lý kết nối cơ sở dữ liệu cũng được tối ưu hóa để đảm bảo tốc độ ghi dữ liệu luôn ở mức cao nhất mà không gây quá tải cho hạ tầng lưu trữ.

Toàn bộ các quy trình từ thu thập, xử lý đến bảo trì hệ thống đều được tự động hóa. Điều này giảm thiểu tối đa sự can thiệp của con người, cho phép hệ thống vận hành liên tục và ổn định trong thời gian dài. Dự án là sự kết hợp giữa tư duy logic chặt chẽ và các giải pháp kỹ thuật hiện đại để xây dựng nên một dòng chảy dữ liệu thông suốt.
