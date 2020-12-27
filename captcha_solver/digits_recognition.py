import argparse
import glob
import os
import cv2
import torch
import torch.nn.functional as F
import torch.optim as optim

from torch.optim.lr_scheduler import StepLR
from torch.utils.data import Dataset, DataLoader

from extract_single_digits import get_single_digits
from model import Net
from utils import load_labels, DATA_PATH, preprocess_digits
from solver import captcha_solver_validation


class DigitDataset(Dataset):
    def __init__(self, digits, labels):
        self.digits = digits
        self.labels = labels

    def __len__(self):
        return len(self.digits)

    def __getitem__(self, item):
        return self.digits[item], self.labels[item]


def collect_digit_dataset(path):
    captcha_labels = load_labels()

    digits, labels = [], []
    for image_path in glob.glob(os.path.join(path, "*.png")):
        image = cv2.imread(image_path)
        file = image_path.split(os.path.sep)[-1]
        image_digits = get_single_digits(image)
        label = captcha_labels.get(file, {'label': None})['label']
        label = [x for x in label]

        if len(label) == len(image_digits):
            digits += image_digits
            labels += label

    # prepocess images
    digits = preprocess_digits(digits)

    # labels
    labels = torch.LongTensor([int(x) for x in labels])

    return DigitDataset(digits, labels)


def train(args, model, device, train_loader, optimizer, epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % args.log_interval == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                       100. * batch_idx / len(train_loader), loss.item()))
            if args.dry_run:
                break


def test(model, device, test_loader):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction='sum').item()  # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader.dataset)

    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
        test_loss, correct, len(test_loader.dataset),
        100. * correct / len(test_loader.dataset)))


def main():
    # Training settings
    parser = argparse.ArgumentParser(description='PyTorch digit recognition model')
    parser.add_argument('--batch-size', type=int, default=64, metavar='N',
                        help='input batch size for training (default: 64)')
    parser.add_argument('--test-batch-size', type=int, default=1000, metavar='N',
                        help='input batch size for testing (default: 1000)')
    parser.add_argument('--epochs', type=int, default=3, metavar='N',
                        help='number of epochs to train (default: 14)')
    parser.add_argument('--lr', type=float, default=1.0, metavar='LR',
                        help='learning rate (default: 1.0)')
    parser.add_argument('--gamma', type=float, default=0.7, metavar='M',
                        help='Learning rate step gamma (default: 0.7)')
    parser.add_argument('--no-cuda', action='store_true', default=False,
                        help='disables CUDA training')
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='quickly check a single pass')
    parser.add_argument('--seed', type=int, default=1, metavar='S',
                        help='random seed (default: 1)')
    parser.add_argument('--log-interval', type=int, default=100, metavar='N',
                        help='how many batches to wait before logging training status')
    parser.add_argument('--save-model', action='store_false', default=True,
                        help='For Saving the current Model')
    args = parser.parse_args()
    use_cuda = not args.no_cuda and torch.cuda.is_available()

    torch.manual_seed(args.seed)

    device = torch.device("cuda" if use_cuda else "cpu")

    kwargs = {'batch_size': args.batch_size}
    if use_cuda:
        kwargs.update({'num_workers': 1,
                       'pin_memory': True,
                       'shuffle': True},
                      )

    valid_path = os.path.join(DATA_PATH, 'annotated_data/valid')
    train_path = os.path.join(DATA_PATH, 'annotated_data/train')

    train_dataset = collect_digit_dataset(train_path)
    valid_dataset = collect_digit_dataset(valid_path)

    train_loader = DataLoader(train_dataset, batch_size=16)
    test_loader = DataLoader(valid_dataset)

    model_path = 'models/mnist_cnn.pt'
    model = torch.load(model_path, map_location=device)

    optimizer = optim.Adadelta(model.parameters(), lr=args.lr)

    scheduler = StepLR(optimizer, step_size=1, gamma=args.gamma)
    for epoch in range(1, args.epochs + 1):
        train(args, model, device, train_loader, optimizer, epoch)
        test(model, device, test_loader)
        scheduler.step()

    if args.save_model:
        torch.save(model, "models/digit_cnn.pt")

    captcha_accuracy, digit_accuracy = captcha_solver_validation(verbose=True)
    print(f'captcha_accuracy: {captcha_accuracy}, digit_accuracy: {digit_accuracy}')


if __name__ == '__main__':
    main()
