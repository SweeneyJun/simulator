clear
algo = 'true';
container = '5c';
data_size = '100g';
host = ['tian02'; 'tian03'; 'tian04'; 'tian05'; 'tian06'; 'tian07'; 'nasa08'; 'nasa09'; 'nasa10'];
prefix = 'tsar-result/';
suffix = '-capture.txt';
link = '-';

total_in = zeros(250,1);
total_out = zeros(250,1);
for i = 9:size(host, 1)
    file_name = strcat(prefix, algo(1,:), link, container, link, data_size, link, host(i, :), suffix);
    data = importdata(file_name);
    end_index = size(data,1);
    for j = size(data,1):-1:1
        if data(j, 3) > 150 || data(j,4) > 150
            end_index = j;
            break;
        end
    end
    end_index
    total_in(1:end_index, 1) = total_in(1:end_index, 1) + data(1:end_index, 1)*8;
    total_out(1:end_index, 1) = total_out(1:end_index, 1) + data(1:end_index, 2)*8;
    figure
    subplot(2,1,1)
    plot(1:end_index, data(1:end_index, 1)*8, 'r','LineWidth', 1.2)
    legend('bits-in')
    hold on
    subplot(2,1,2)
    plot(1:end_index, data(1:end_index, 2)*8, 'b','LineWidth', 1.2)
    legend('bits-out')
    bits_in(i) = max(data(1:end_index, 1))*8;
    bits_out(i) = max(data(1:end_index, 2))*8;
end

% figure
% plot(1:250, total_in, 'r')
% hold on
% plot(1:250, total_out, 'b')