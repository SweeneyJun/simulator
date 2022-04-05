color=[
    198,78,43;    %红色
    133,179,26;       %绿色
    37,141,211;      %青色
    143,63,143;       %紫色
    38,87,153;       %蓝色
    228,209,39;      %黄色
    ]/255;  

node = 10:10:100;
maxReducer = 5;
maxLink = 5;
epsilon = 0.00001;
recordBand = zeros(100, maxReducer, size(node, 2));
averageBand = zeros(maxReducer, size(node, 2));
for t = 1: 200
    for i = 1: size(node, 2)
        inBand = zeros(1, node(i));
        outBand = zeros(1, node(i));
        for j = 1: maxReducer
            inRemainder = ones(1, node(i)) - inBand;
            outRemainder = ones(1, node(i)) - outBand;
            outNum = zeros(1, node(i));
            links = zeros(node(i), node(i));
            for n = 1: node(i)
                links(n, :) = randperm(node(i));
                for l = 1: maxLink
                    if links(n, l) ~= n
                        outNum(links(n, l)) = outNum(links(n, l)) + 1;
                    else
                        outNum(links(n, maxLink + 1)) = outNum(links(n, maxLink + 1)) + 1;
                    end
                end
            end
            for n = 1: node(i)
                if inRemainder(n) < epsilon
                    continue
                end
                for l = 1: maxLink
                    if outRemainder(links(n, l)) < epsilon
                        continue
                    end
                    if outRemainder(links(n, l))/outNum(links(n, l)) < inRemainder(n)/maxLink
                        outBand(links(n, l)) = outBand(links(n, l)) + outRemainder(links(n, l))/outNum(links(n, l));
                        inBand(n) = inBand(n) + outRemainder(links(n, l))/outNum(links(n, l));
                    else
                        outBand(links(n, l)) = outBand(links(n, l)) + inRemainder(n)/maxLink;
                        inBand(n) = inBand(n) + inRemainder(n)/maxLink;
                    end
                end
            end
            recordBand(t, j, i) = mean(inBand);
        end
    end
end
for i = 1: size(node, 2)
    for j = 1: maxReducer
        averageBand(j,i) = mean(recordBand(:,j,i));
    end
end

plot(node, averageBand(1, :),'-h','MarkerSize',8,'LineWidth',2,'Color',color(1,:))
hold on

plot(node, averageBand(2, :),'-p','MarkerSize',8,'LineWidth',2,'Color',color(2,:))
hold on 
plot(node, averageBand(3, :),'-o','MarkerSize',8,'LineWidth',2,'Color',color(3,:))
hold on
plot(node, averageBand(4, :),'-v','MarkerSize',8,'LineWidth',2,'Color',color(4,:))
hold on 
plot(node, averageBand(5, :),'-square','MarkerSize',8,'LineWidth',2,'Color',color(5,:))
hold on
% plot(node, averageBand(6, :),'-d','MarkerSize',8,'LineWidth',2,'Color',color(6,:))
% hold on

xlim([0 110]);
ylim([0.825 0.988]);

str1='\fontsize {15}\fontname {Helvetica}1 transfer task';
str2='\fontsize {15}\fontname {Helvetica}2 transfer tasks';
str3='\fontsize {15}\fontname {Helvetica}3 transfer tasks';
str4='\fontsize {15}\fontname {Helvetica}4 transfer tasks';
str5='\fontsize {15}\fontname {Helvetica}5 transfer tasks';
hleg=legend(str1,str2,str3,str4,str5);
% set(hleg,'Location','northwest');
vector = [.7,.37,0,0];% [xposition yposition width height]
set(hleg,'Position', vector)
set(gca,'FontSize',19);
% 
fh=figure(1);
set(fh, 'color', 'white'); 
xlabel('\fontsize {22}\fontname {Helvetica}Number of Nodes');
ylabel('\fontsize {22}\fontname {Helvetica}Average Bandwidth(%)');
grid on;
% 
set(gcf, 'PaperPosition', [0 0 18 12]); %Position plot at left hand corner with width 5 and height 5.
set(gcf, 'PaperSize', [18 12]); %Set the paper to have width 5 and height 5.
saveas(gcf,'reducerTest.pdf');
