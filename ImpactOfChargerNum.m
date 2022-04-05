% clear all;
% clc
% side_length=200;
% region=[0 side_length 0 side_length];
% 
% a=0;
% if a==1
%     charger=side_length*rand(150,2);
%     device=side_length*rand(1000,2);
%     save('points_charger_num.mat','charger','device');
% else
%     p=load('points_charger_num.mat');
%     charger=p.charger;
%     device=p.device;
% end
% % alpha_mu=75;
% % beta_mu=40;
% % alpha_sigma=50;
% % beta_sigma=20;
% alpha_mu=75;
% beta_mu=40;
% alpha_sigma=50;
% beta_sigma=20;
% D=13;
% epsilon=0.3;
% Rt=0.08;
% yita = 0.6;
% robust_charging_utility = zeros(14, 1);
% greedy_charging_value = zeros(14, 1);
% cosntant_charging_utility = zeros(14, 1);
% cosntant_charging_utility_13 = zeros(14, 1);
% centralize_utility = zeros(14, 1);
% global_utility = zeros(14, 1);
% m=ceil((1+sqrt(1-epsilon))/(epsilon));
% tic
% % write_mark = 1;
% for i = 1:14
%     index = 10+i*10;
%     [robust_charging_utility(i)] = RobustNearOptimalAlgorithm( charger(1:index,:), device, alpha_mu, beta_mu, alpha_sigma, beta_sigma, Rt, D, region, epsilon, yita, m, 1 );
%     [greedy_charging_value(i), centralize_utility(i)]=GreedyAlgorithm(charger(1:index,:),device,alpha_mu,beta_mu,alpha_sigma,beta_sigma,Rt,D,epsilon,yita,1);
%     [cosntant_charging_utility(i)]=ConstantAlgorithm(charger(1:index,:),device,region,alpha_mu,beta_mu,alpha_sigma,beta_sigma,Rt,D,epsilon,yita,1);
%     [cosntant_charging_utility_13(i)]=ConstantAlgorithm_13(charger(1:index,:),device,region,alpha_mu,beta_mu,alpha_sigma,beta_sigma,Rt,D,epsilon,yita,1);
%     [~, global_utility(i)]=GreedyAlgorithm(charger(1:index,:),device,alpha_mu,beta_mu,alpha_sigma,beta_sigma,Rt,D,0.05,yita,1);
% end
% 
% toc

clear all

b = 1;
if b == 1
    data1 = load('charger_num_0130_1.mat');
    data2 = load('charger_num_0130.mat');
else
    save('charger_num_temp_0721.mat','greedy_charging_value','centralize_utility', 'robust_charging_utility', 'cosntant_charging_utility','cosntant_charging_utility_13','global_utility');
end

global_utility(1:5) = data1.global_utility(1:5);
global_utility(6:9) = data2.global_utility(6:9);
centralize_utility(1:5) = data1.centralize_utility(1:5);
centralize_utility(6:9) = data2.centralize_utility(6:9);
robust_charging_utility(1:5) = data1.robust_charging_utility(1:5);
robust_charging_utility(6:9) = data2.robust_charging_utility(6:9);
greedy_charging_value(1:5) = data1.greedy_charging_value(1:5);
greedy_charging_value(6:9) = data2.greedy_charging_value(6:9);
cosntant_charging_utility_13(1:5) = data1.cosntant_charging_utility_13(1:5);
cosntant_charging_utility_13(6:9) = data2.cosntant_charging_utility_13(6:9);
cosntant_charging_utility(1:5) = data1.cosntant_charging_utility(1:5);
cosntant_charging_utility(6:9) = data2.cosntant_charging_utility(6:9);

color=[
    198,78,43;    %红色
    133,179,26;       %绿色
    37,141,211;      %青色
    143,63,143;       %紫色
    38,87,153;       %蓝色
    228,209,39;      %黄色
    ]/255;    


x = (1:9)*15;
figure
h = plot(x,global_utility,'-o','MarkerSize',8,'LineWidth',2,'Color',color(1,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,centralize_utility,'-p','MarkerSize',8,'LineWidth',2,'Color',color(2,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,robust_charging_utility,'-h','MarkerSize',8,'LineWidth',2,'Color',color(3,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,greedy_charging_value,'-v','MarkerSize',8,'LineWidth',2,'Color',color(4,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,cosntant_charging_utility_13,'-square','MarkerSize',8,'LineWidth',2,'Color',color(5,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,cosntant_charging_utility,'-d','MarkerSize',8,'LineWidth',2,'Color',color(6,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on


% plot(number,overall_utility_near_optimal_new,'k-d','markerfacecolor','k','MarkerSize',8,'LineWidth',2);
% hold on;
% xlim([min(number) max(number)]);
% ylim([0 40]);
%set(gca, 'XTick', Epsilon)% X坐标轴刻度数据点位置
hold on;

str1='\fontsize {15}\fontname {Helvetica}Optimal';
str2='\fontsize {15}\fontname {Helvetica}Centralized ROSE';
str3='\fontsize {15}\fontname {Helvetica}Distributed ROSE';
str4='\fontsize {15}\fontname {Helvetica}Greedy';
str5='\fontsize {15}\fontname {Helvetica}1/3 Approximation';
str6='\fontsize {15}\fontname {Helvetica}1/4 Approximation';
% ylim([-3 28])
% xlim([0 165])
% str7='\fontsize {14}\fontname {Helvetica}New Algorithm';
hleg=legend(str1,str2,str3,str4,str5,str6);
set(hleg,'Location','Southeast');
hold on;
set(gca,'FontSize',19);
%Grid on;

fh=figure(1);
set(fh, 'color', 'white'); 
xlabel('\fontsize {22}\fontname {Helvetica} Number of Chargers');
ylabel('\fontsize {22}\fontname {Helvetica} Charging Utility');
grid on;

% set(gcf, 'PaperPosition', [0 0 18 12]); %Position plot at left hand corner with width 5 and height 5.
% set(gcf, 'PaperSize', [18 12]); %Set the paper to have width 5 and height 5.
% saveas(gcf,'ChargerNum.pdf');