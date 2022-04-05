color=[
        255,0,0;
    0,0,255;
    0,255,0;
    255,255,0;
%     0,0,0;
%     0,0,0;
    0,255,255;
%     0,0,0;
%     0,0,0;
    255,0,255;
%     0,0,0;
%     0,0,0;
    128,0,255;
%     0,0,0;
%     0,0,0;
    128,255,0;
%     0,0,0;
%     0,0,0;
    0,128,255;

%     0,0,0;
%     0,0,0;
    198,78,43;    %红色
%     0,0,0;
%     0,0,0;
    133,179,26;     %绿色
%     0,0,0;
%     0,0,0;
    37,141,211;      %青色
%     0,0,0;
%     0,0,0;
    143,63,143;       %紫色
    0,0,0;
    0,0,0;
    38,87,153;       %蓝色
    0,0,0;
    0,0,0;
    228,209,39;      %黄色
    0,0,0;
    0,0,0;
    128,255,0;
    0,0,0;
    0,0,0;
    0,128,255;
    0,0,0;
    0,0,0;
    198,78,43;    %红色
    0,0,0;
    0,0,0;
    133,179,26;     %绿色
    0,0,0;
    0,0,0;
    37,141,211;      %青色
    0,0,0;
    0,0,0;
    143,63,143;       %紫色
    0,0,0;
    0,0,0;
    38,87,153;       %蓝色
    0,0,0;
    0,0,0;
    228,209,39;      %黄色
    0,0,0;
    0,0,0;
%  255,0,0;
%     0,0,255;
%     0,255,0;
%     255,255,0;
%     0,255,255;
% %     0,0,0;
% %     0,0,0;
%     255,0,255;
% %     0,0,0;
% %     0,0,0;
%     128,0,255;
        255,0,0;
    0,0,255;
    0,255,0;
    255,255,0;
    0,0,0;
    0,0,0;
    0,255,255;
    0,0,0;
    0,0,0;
    255,0,255;
    0,0,0;
    0,0,0;
    128,0,255;
    0,0,0;
    0,0,0;
    128,255,0;
    0,0,0;
    0,0,0;
    0,128,255;

    0,0,0;
    0,0,0;
    198,78,43;    %红色
    0,0,0;
    0,0,0;
    133,179,26;     %绿色
    0,0,0;
    0,0,0;
    37,141,211;      %青色
    0,0,0;
    0,0,0;
    143,63,143;       %紫色
    0,0,0;
    0,0,0;
    38,87,153;       %蓝色
    0,0,0;
    0,0,0;
    228,209,39;      %黄色
    0,0,0;
    0,0,0;
    128,255,0;
    0,0,0;
    0,0,0;
    0,128,255;
    0,0,0;
    0,0,0;
    198,78,43;    %红色
    0,0,0;
    0,0,0;
    133,179,26;     %绿色
    0,0,0;
    0,0,0;
    37,141,211;      %青色
    0,0,0;
    0,0,0;
    143,63,143;       %紫色
    0,0,0;
    0,0,0;
    38,87,153;       %蓝色
    0,0,0;
    0,0,0;
    228,209,39;      %黄色
    0,0,0;
    0,0,0;
    
            255,0,0;
    0,0,255;
    0,255,0;
    255,255,0;
    0,0,0;
    0,0,0;
    0,255,255;
    0,0,0;
    0,0,0;
    255,0,255;
    0,0,0;
    0,0,0;
    128,0,255;
    0,0,0;
    0,0,0;
    128,255,0;
    0,0,0;
    0,0,0;
    0,128,255;

    0,0,0;
    0,0,0;
    198,78,43;    %红色
    0,0,0;
    0,0,0;
    133,179,26;     %绿色
    0,0,0;
    0,0,0;
    37,141,211;      %青色
    0,0,0;
    0,0,0;
    143,63,143;       %紫色
    0,0,0;
    0,0,0;
    38,87,153;       %蓝色
    0,0,0;
    0,0,0;
    228,209,39;      %黄色
    0,0,0;
    0,0,0;
    128,255,0;
    0,0,0;
    0,0,0;
    0,128,255;
    0,0,0;
    0,0,0;
    198,78,43;    %红色
    0,0,0;
    0,0,0;
    133,179,26;     %绿色
    0,0,0;
    0,0,0;
    37,141,211;      %青色
    0,0,0;
    0,0,0;
    143,63,143;       %紫色
    0,0,0;
    0,0,0;
    38,87,153;       %蓝色
    0,0,0;
    0,0,0;
    228,209,39;      %黄色
    0,0,0;
    0,0,0;

    ]/255; 

% data = importdata('schedule.txt');
data = importdata('RM-log-analyze.txt');

nSlots = 10;
nHosts = 9;
slot_time = zeros(nSlots * nHosts, 1);
figure

% x = data(:,1) > 0;
% data(x,1) = 1;

for i = 1: size(data, 1)
%     if data(i,1) < 5
%         continue
%     end
    if data(i, 6) == -1
        host = data(i, 3);
        start_time = round(data(i, 4));
        end_time = round(data(i, 5));
        time = start_time : 1 : end_time;
        slot = 0;
        for j = host * nSlots + 1: (host + 1) * nSlots
%             if i == 219
%                 slot_time(j)
%                 start_time
%             end
            if slot_time(j) <= start_time
                slot =  j;
                slot_time(slot) = end_time;
                break;
            end
        end
        if slot == 0
            i
        end
        plot(time, slot*ones(size(time,1), size(time,2)), 'color', color(data(i,1)*3 + 1, :), 'LineWidth', 2)
        hold on
        plot(time(1), slot, 'k.','markersize', 12)
        hold on
        plot(time(end), slot, 'k.', 'markersize', 12)
        hold on
    else
        host = data(i, 3);
        start_time = round(data(i, 4));
        shuffle_end_time = round(data(i, 5));
        end_time = round(data(i, 6));
        time = start_time : 1 : shuffle_end_time;
        for j = host * nSlots + 1: (host + 1) * nSlots
            if slot_time(j) <= start_time
                slot =  j;
                slot_time(slot) = end_time;
                break;
            end
        end
        plot(time, slot*ones(size(time,1), size(time,2)),  'color', color(data(i,1)*3 + 2, :), 'LineWidth', 2)
        hold on
        time = shuffle_end_time : 1: end_time;
        plot(time, slot*ones(size(time,1), size(time,2)),  'color', color(data(i,1)*3 + 3, :), 'LineWidth', 2)
        hold on
    end
end
for i = 1: nHosts - 1
    plot(1:max(data(:,6)), (nSlots*i + 0.5)*ones(floor(max(data(:,6))), 1), 'k--', 'LineWidth', 2)
    hold on
end
axis([0,max(max(data(:,6)), max(data(:,5))), 0, nSlots*nHosts + 1])
% 
% set(gcf, 'PaperPosition', [0 0 24 16]); %Position plot at left hand corner with width 5 and height 5.
% set(gcf, 'PaperSize', [24 16]); %Set the paper to have width 5 and height 5.
% saveas(gcf,'10c-true-1mrjob-200g.pdf');
%         