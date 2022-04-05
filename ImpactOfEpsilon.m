% clear all;
% clc
% side_length=100;
% region=[0 side_length 0 side_length];
% 
% a=0;
% if a==1
%     charger=side_length*rand(30,2);
%     device=side_length*rand(1000,2);
%     save('points_yita.mat','charger','device');
% else
%     p=load('points_yita.mat');
%     charger=p.charger;
%     device=p.device;
% end
% % alpha_mu=75;
% % beta_mu=40;
% % alpha_sigma=50;
% % beta_sigma=20;
% alpha_mu=60;
% beta_mu=40;
% alpha_sigma=50;
% beta_sigma=20;
% D=13;
epsilon=0.1:0.03:0.3;
% Rt=0.08;
% yita = 0.6;
% robust_charging_utility = zeros(size(epsilon,2), 1);
% greedy_charging_value = zeros(size(epsilon,2), 1);
% cosntant_charging_utility = zeros(size(epsilon,2), 1);
% cosntant_charging_utility_13 = zeros(size(epsilon,2), 1);
% centralize_utility = zeros(size(epsilon,2), 1);
% global_utility = zeros(size(epsilon,2), 1);
% m=ceil((1+sqrt(1-epsilon))/(epsilon));
% tic
% % write_mark = 1;
% for i = 1:size(epsilon,2)
%     i
%     [robust_charging_utility(i)] = RobustNearOptimalAlgorithm( charger, device, alpha_mu, beta_mu, alpha_sigma, beta_sigma, Rt, D, region, epsilon(i), yita, m, 1 );
%     [greedy_charging_value(i), centralize_utility(i)]=GreedyAlgorithm(charger,device,alpha_mu,beta_mu,alpha_sigma,beta_sigma,Rt,D,epsilon(i),yita,0);
%     [cosntant_charging_utility(i)]=ConstantAlgorithm(charger,device,region,alpha_mu,beta_mu,alpha_sigma,beta_sigma,Rt,D,epsilon(i),yita,0);
%     [cosntant_charging_utility_13(i)]=ConstantAlgorithm_13(charger,device,region,alpha_mu,beta_mu,alpha_sigma,beta_sigma,Rt,D,epsilon(i),yita,0);
%     if i == 1
%         [~, global_utility(i)]=GreedyAlgorithm(charger,device,alpha_mu,beta_mu,alpha_sigma,beta_sigma,Rt,D,0.05,yita,1);
%     else
%         global_utility(i) == global_utility(1);
%     end
%     save('epsilon_temp.mat','greedy_charging_value','centralize_utility', 'robust_charging_utility', 'cosntant_charging_utility','cosntant_charging_utility_13','global_utility');
% end
% 
% toc
b = 1;
if b == 1
%     load('epsilon.mat');
    load('epsilon_0202.mat');
else
    save('epsilon.mat','greedy_charging_value','centralize_utility', 'robust_charging_utility', 'cosntant_charging_utility','cosntant_charging_utility_13','global_utility');
end

color=[
    198,78,43;    %红色
    133,179,26;       %绿色
    37,141,211;      %青色
    143,63,143;       %紫色
    38,87,153;       %蓝色
    228,209,39;      %黄色
    ]/255;    
global_utility(:) = global_utility(1);
x = epsilon;
figure
h = plot(x,global_utility,'-o','MarkerSize',10,'LineWidth',2,'Color',color(1,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,centralize_utility,'-p','MarkerSize',10,'LineWidth',2,'Color',color(2,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,robust_charging_utility,'-h','MarkerSize',10,'LineWidth',2,'Color',color(3,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,greedy_charging_value,'-v','MarkerSize',10,'LineWidth',2,'Color',color(4,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,cosntant_charging_utility_13,'-square','MarkerSize',10,'LineWidth',2,'Color',color(5,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on
h = plot(x,cosntant_charging_utility,'-d','MarkerSize',10,'LineWidth',2,'Color',color(6,:));%g
set(h, 'MarkerFaceColor', get(h, 'Color'));
hold on

% plot(number,overall_utility_near_optimal_new,'k-d','markerfacecolor','k','MarkerSize',8,'LineWidth',2);
% hold on;
xlim([0.08 0.3]);
ylim([0.65 3.4]);
%set(gca, 'XTick', Epsilon)% X坐标轴刻度数据点位置
hold on;

str1='\fontsize {15}\fontname {Helvetica}Optimal';
str2='\fontsize {15}\fontname {Helvetica}Centralized ROSE';
str3='\fontsize {15}\fontname {Helvetica}Distributed ROSE';
str4='\fontsize {15}\fontname {Helvetica}Greedy';
str5='\fontsize {15}\fontname {Helvetica}1/3 Approximation';
str6='\fontsize {15}\fontname {Helvetica}1/4 Approximation';
% str7='\fontsize {14}\fontname {Helvetica}New Algorithm';
hleg=legend(str1,str2,str3,str4,str5,str6);
% set(hleg,'Fontsize',12)
vector = [.283,.498,0,0];% [xposition yposition width height]
set(hleg,'Position', vector)
hold on;
set(gca,'FontSize',19);
%Grid on;

fh=figure(1);
set(fh, 'color', 'white'); 
xlabel(['\fontsize {22}\fontname {Helvetica} Threhold \epsilon']);
ylabel('\fontsize {22}\fontname {Helvetica} Charging Utility');
grid on;

set(gcf, 'PaperPosition', [0 0 18 12]); %Position plot at left hand corner with width 5 and height 5.
set(gcf, 'PaperSize', [18 12]); %Set the paper to have width 5 and height 5.
saveas(gcf,'Epsilon_0202.pdf');
